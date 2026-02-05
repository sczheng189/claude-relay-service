const redis = require('../models/redis')
const logger = require('../utils/logger')
const { isClaudeFamilyModel } = require('../utils/modelHelper')

function pad2(n) {
  return String(n).padStart(2, '0')
}

// 生成配置时区下的 YYYY-MM-DD 字符串。
// 注意：入参 date 必须是 redis.getDateInTimezone() 生成的“时区偏移后”的 Date。
function formatTzDateYmd(tzDate) {
  return `${tzDate.getUTCFullYear()}-${pad2(tzDate.getUTCMonth() + 1)}-${pad2(tzDate.getUTCDate())}`
}

class WeeklyClaudeCostInitService {
  _getCurrentWeekDatesInTimezone() {
    const tzNow = redis.getDateInTimezone(new Date())
    const tzToday = new Date(tzNow)
    tzToday.setUTCHours(0, 0, 0, 0)

    // ISO 周：周一=1 ... 周日=7
    const isoDay = tzToday.getUTCDay() || 7
    const tzMonday = new Date(tzToday)
    tzMonday.setUTCDate(tzToday.getUTCDate() - (isoDay - 1))

    const dates = []
    for (let d = new Date(tzMonday); d <= tzToday; d.setUTCDate(d.getUTCDate() + 1)) {
      dates.push(formatTzDateYmd(d))
    }
    return dates
  }

  _buildWeeklyClaudeKey(keyId, weekString) {
    return `usage:claude:weekly:${keyId}:${weekString}`
  }

  /**
   * 自动迁移旧 Redis 字段：weeklyOpusCostLimit → weeklyClaudeCostLimit
   * 以及 usage:opus:* → usage:claude:* 键名。
   * 幂等安全：新字段已存在时跳过，迁移完成后写 done 标记避免重复执行。
   */
  async _migrateOpusToClaudeFields() {
    const client = redis.getClientSafe()
    if (!client) return

    const doneKey = 'migrate:opus_to_claude:done'
    try {
      const alreadyDone = await client.get(doneKey)
      if (alreadyDone) return
    } catch {
      // 读取失败不阻断
    }

    logger.info('🔄 检测到首次使用新版本，自动迁移 opus → claude 字段...')
    let migrated = 0

    // UUID 格式正则：apikey:{uuid} 形式的键才是实际的 API Key 数据
    const uuidPattern = /^apikey:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

    try {
      // 1. 迁移 API Key Hash 字段
      let cursor = '0'
      do {
        const [nextCursor, keys] = await client.scan(cursor, 'MATCH', 'apikey:*', 'COUNT', 500)
        cursor = nextCursor
        for (const key of keys) {
          // 跳过索引键（如 apikey:idx:*, apikey:set:*, apikey:hash_map 等），只处理实际的 API Key 数据
          if (!uuidPattern.test(key)) {
            continue
          }
          const oldValue = await client.hget(key, 'weeklyOpusCostLimit')
          if (oldValue !== null) {
            const newValue = await client.hget(key, 'weeklyClaudeCostLimit')
            if (newValue === null) {
              await client.hset(key, 'weeklyClaudeCostLimit', oldValue)
            }
            await client.hdel(key, 'weeklyOpusCostLimit')
            migrated++
          }
        }
      } while (cursor !== '0')

      // 2. 迁移 usage:opus:* 键（weekly/total 及其 real 变体）
      const keyPatterns = [
        {
          pattern: 'usage:opus:weekly:*',
          old: 'usage:opus:weekly:',
          new: 'usage:claude:weekly:',
          preserveTtl: true
        },
        {
          pattern: 'usage:opus:real:weekly:*',
          old: 'usage:opus:real:weekly:',
          new: 'usage:claude:real:weekly:',
          preserveTtl: true
        },
        {
          pattern: 'usage:opus:total:*',
          old: 'usage:opus:total:',
          new: 'usage:claude:total:',
          preserveTtl: false
        },
        {
          pattern: 'usage:opus:real:total:*',
          old: 'usage:opus:real:total:',
          new: 'usage:claude:real:total:',
          preserveTtl: false
        }
      ]

      for (const kp of keyPatterns) {
        cursor = '0'
        do {
          const [nextCursor, keys] = await client.scan(cursor, 'MATCH', kp.pattern, 'COUNT', 500)
          cursor = nextCursor
          for (const key of keys) {
            const newKey = key.replace(kp.old, kp.new)
            const exists = await client.exists(newKey)
            if (!exists) {
              const value = await client.get(key)
              if (kp.preserveTtl) {
                const ttl = await client.ttl(key)
                if (ttl > 0) {
                  await client.set(newKey, value, 'EX', ttl)
                } else {
                  await client.set(newKey, value)
                }
              } else {
                await client.set(newKey, value)
              }
            }
            await client.del(key)
            migrated++
          }
        } while (cursor !== '0')
      }

      // 写 done 标记（永不过期）
      await client.set(doneKey, new Date().toISOString())

      if (migrated > 0) {
        logger.info(`✅ opus → claude 字段迁移完成：${migrated} 个字段/键已处理`)
      } else {
        logger.info('✅ opus → claude 字段迁移完成：无需迁移的数据')
      }
    } catch (error) {
      logger.warn(
        '⚠️ opus → claude 字段迁移出错（不影响启动）:',
        error.message || error.code || error
      )
    }
  }

  /**
   * 启动回填：把"本周（周一到今天）Claude 全模型"周费用从按日/按模型统计里反算出来，
   * 写入 `usage:claude:weekly:*`，保证周限额在重启后不归零。
   *
   * 说明：
   * - 只回填本周，不做历史回填（符合"只要本周数据"诉求）
   * - 会加分布式锁，避免多实例重复跑
   * - 会写 done 标记：同一周内重启默认不重复回填（需要时可手动删掉 done key）
   */
  async backfillCurrentWeekClaudeCosts() {
    const client = redis.getClientSafe()
    if (!client) {
      logger.warn('⚠️ 本周 Claude 周费用回填跳过：Redis client 不可用')
      return { success: false, reason: 'redis_unavailable' }
    }

    // 先执行旧字段迁移（幂等，只在首次升级时实际执行）
    await this._migrateOpusToClaudeFields()

    const weekString = redis.getWeekStringInTimezone()
    const doneKey = `init:weekly_claude_cost:${weekString}:done`

    try {
      const alreadyDone = await client.get(doneKey)
      if (alreadyDone) {
        logger.info(`ℹ️ 本周 Claude 周费用回填已完成（${weekString}），跳过`)
        return { success: true, skipped: true }
      }
    } catch (e) {
      // 尽力而为：读取失败不阻断启动回填流程。
    }

    const lockKey = `lock:init:weekly_claude_cost:${weekString}`
    const lockValue = `${process.pid}:${Date.now()}`
    const lockTtlMs = 15 * 60 * 1000

    const lockAcquired = await redis.setAccountLock(lockKey, lockValue, lockTtlMs)
    if (!lockAcquired) {
      logger.info(`ℹ️ 本周 Claude 周费用回填已在运行（${weekString}），跳过`)
      return { success: true, skipped: true, reason: 'locked' }
    }

    const startedAt = Date.now()
    try {
      logger.info(`💰 开始回填本周 Claude 周费用：${weekString}（仅本周）...`)

      const keyIds = await redis.scanApiKeyIds()
      const dates = this._getCurrentWeekDatesInTimezone()

      // 预加载所有 API Key 数据（避免循环内重复查询）
      const keyDataCache = new Map()
      const batchSize = 500
      for (let i = 0; i < keyIds.length; i += batchSize) {
        const batch = keyIds.slice(i, i + batchSize)
        const pipeline = client.pipeline()
        for (const keyId of batch) {
          pipeline.hgetall(`apikey:${keyId}`)
        }
        const results = await pipeline.exec()
        for (let j = 0; j < batch.length; j++) {
          const [, data] = results[j] || []
          if (data && Object.keys(data).length > 0) {
            keyDataCache.set(batch[j], data)
          }
        }
      }
      logger.info(`💰 预加载 ${keyDataCache.size} 个 API Key 数据`)

      // 推断账户类型的辅助函数（与运行时 recordClaudeCost 一致，只统计 claude-official/claude-console/ccr）
      const CLAUDE_ACCOUNT_TYPES = ['claude-official', 'claude-console', 'ccr']
      const inferAccountType = (keyData) => {
        if (keyData?.ccrAccountId) {
          return 'ccr'
        }
        if (keyData?.claudeConsoleAccountId) {
          return 'claude-console'
        }
        if (keyData?.claudeAccountId) {
          return 'claude-official'
        }
        // bedrock/azure/gemini 等不计入周费用
        return null
      }

      const costByKeyId = new Map()
      let scannedKeys = 0
      let matchedClaudeKeys = 0

      const toInt = (v) => {
        const n = parseInt(v || '0', 10)
        return Number.isFinite(n) ? n : 0
      }

      // 扫描“按日 + 按模型”的使用统计 key，并反算 Claude 系列模型的费用。
      for (const dateStr of dates) {
        let cursor = '0'
        const pattern = `usage:*:model:daily:*:${dateStr}`

        do {
          const [nextCursor, keys] = await client.scan(cursor, 'MATCH', pattern, 'COUNT', 1000)
          cursor = nextCursor
          scannedKeys += keys.length

          const entries = []
          for (const usageKey of keys) {
            // usage:{keyId}:model:daily:{model}:{YYYY-MM-DD}
            const match = usageKey.match(/^usage:([^:]+):model:daily:(.+):(\d{4}-\d{2}-\d{2})$/)
            if (!match) {
              continue
            }
            const keyId = match[1]
            const model = match[2]
            if (!isClaudeFamilyModel(model)) {
              continue
            }
            matchedClaudeKeys++
            entries.push({ usageKey, keyId, model })
          }

          if (entries.length === 0) {
            continue
          }

          const pipeline = client.pipeline()
          for (const entry of entries) {
            pipeline.hgetall(entry.usageKey)
          }
          const results = await pipeline.exec()

          for (let i = 0; i < entries.length; i++) {
            const entry = entries[i]
            const [, data] = results[i] || []
            if (!data || Object.keys(data).length === 0) {
              continue
            }

            // 直接使用已存储的 ratedCostMicro（已包含倍率），避免重新计算导致精度差异
            const ratedCostMicro = toInt(data.ratedCostMicro)
            if (ratedCostMicro <= 0) {
              continue
            }

            // 转换为美元（micro = 百万分之一）
            const ratedCost = ratedCostMicro / 1000000

            // 验证账户类型：只统计 claude-official/claude-console/ccr 账户
            const keyData = keyDataCache.get(entry.keyId)
            const accountType = inferAccountType(keyData)

            // 与运行时 recordClaudeCost 一致：只统计 claude-official/claude-console/ccr 账户
            if (!accountType || !CLAUDE_ACCOUNT_TYPES.includes(accountType)) {
              continue
            }

            // ratedCostMicro 已包含全局倍率和 Key 倍率，直接累加
            costByKeyId.set(entry.keyId, (costByKeyId.get(entry.keyId) || 0) + ratedCost)
          }
        } while (cursor !== '0')
      }

      // 为所有 API Key 写入本周 claude:weekly key
      const ttlSeconds = 14 * 24 * 3600
      for (let i = 0; i < keyIds.length; i += batchSize) {
        const batch = keyIds.slice(i, i + batchSize)
        const pipeline = client.pipeline()
        for (const keyId of batch) {
          const weeklyKey = this._buildWeeklyClaudeKey(keyId, weekString)
          const cost = costByKeyId.get(keyId) || 0
          pipeline.set(weeklyKey, String(cost))
          pipeline.expire(weeklyKey, ttlSeconds)
        }
        await pipeline.exec()
      }

      // 写入 done 标记（保留略长于 1 周，避免同一周内重启重复回填）。
      await client.set(doneKey, new Date().toISOString(), 'EX', 10 * 24 * 3600)

      const durationMs = Date.now() - startedAt
      logger.info(
        `✅ 本周 Claude 周费用回填完成（${weekString}）：keys=${keyIds.length}, scanned=${scannedKeys}, matchedClaude=${matchedClaudeKeys}, filled=${costByKeyId.size}（${durationMs}ms）`
      )

      return {
        success: true,
        weekString,
        keyCount: keyIds.length,
        scannedKeys,
        matchedClaudeKeys,
        filledKeys: costByKeyId.size,
        durationMs
      }
    } catch (error) {
      logger.error(`❌ 本周 Claude 周费用回填失败（${weekString}）：`, error)
      return { success: false, error: error.message }
    } finally {
      await redis.releaseAccountLock(lockKey, lockValue)
    }
  }
}

module.exports = new WeeklyClaudeCostInitService()
