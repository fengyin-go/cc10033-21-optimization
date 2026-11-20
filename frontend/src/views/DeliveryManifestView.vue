<template>
  <section class="page">
    <header class="page-head">
      <div>
        <h2>ETL 分区交付清单</h2>
        <p class="page-desc">
          一次交付固定“转换规则 + 源批次 + 模型结构”，流水线按清单推进各分区；试运行只核对，不写入目标库。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="refresh-all" type="button" @click="reloadAll">刷新</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p v-if="message" class="notice" :class="{ 'notice-error': messageError }" data-testid="notice">
      {{ message }}
    </p>

    <div class="delivery-grid">
      <form class="panel" @submit.prevent="submitManifest">
        <h3>生成交付清单</h3>
        <label class="field">
          <span>ETL 映射规则</span>
          <select v-model.number="form.rule_id" data-testid="create-rule">
            <option v-for="rule in rules" :key="String(rule.id)" :value="Number(rule.id)">
              {{ rule.规则名称 }} → {{ rule.目标表 }}
            </option>
          </select>
        </label>
        <label class="field">
          <span>源批次</span>
          <select v-model="form.source_batch" data-testid="create-batch">
            <option v-for="batch in batches" :key="batch.源批次" :value="batch.源批次">
              {{ batch.源批次 }}（{{ batch.目标表 }}，{{ batch.记录数 }} 条）
            </option>
          </select>
        </label>
        <label class="field">
          <span>部署环境</span>
          <select v-model="form.environment" data-testid="create-env">
            <option v-for="env in environments" :key="env" :value="env">{{ env }}</option>
          </select>
        </label>
        <button class="btn primary" data-testid="create-manifest" type="submit" :disabled="busy">
          生成清单
        </button>
      </form>

      <section class="panel">
        <h3>目标表结构</h3>
        <p class="muted">新增字段只升级结构版本，已交付分区保留原字段归属；重新交付时整分区覆盖。</p>
        <table class="mini-table">
          <thead>
            <tr><th>目标表</th><th>结构版本</th><th>必填字段</th><th>实际可用字段</th></tr>
          </thead>
          <tbody>
            <tr v-for="target in targets" :key="target.表名" :data-testid="`target-${target.表名}`">
              <td>{{ target.表名 }}</td>
              <td><code>{{ target.当前结构版本 }}</code></td>
              <td>{{ target.当前必填字段.join('、') }}</td>
              <td>{{ target.实际可用字段.join('、') }}</td>
            </tr>
          </tbody>
        </table>
        <form class="inline-form" @submit.prevent="submitSchema">
          <label class="field">
            <span>目标表</span>
            <select v-model="schemaForm.table" data-testid="schema-table">
              <option v-for="target in targets" :key="target.表名" :value="target.表名">
                {{ target.表名 }}
              </option>
            </select>
          </label>
          <label class="field">
            <span>新增字段（逗号分隔）</span>
            <input v-model="schemaForm.fields" data-testid="schema-fields" placeholder="channel_code" />
          </label>
          <label class="field">
            <span>必填字段（逗号分隔，可留空）</span>
            <input v-model="schemaForm.required" data-testid="schema-required" placeholder="order_id,dt" />
          </label>
          <button class="btn" data-testid="schema-submit" type="submit" :disabled="busy">
            更新结构
          </button>
        </form>
      </section>
    </div>

    <table class="data-table">
      <thead>
        <tr>
          <th>清单编号</th><th>目标表</th><th>源批次</th><th>规则版本</th>
          <th>模型结构版本</th><th>分区</th><th>状态</th><th>试运行</th><th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="manifest in manifests"
          :key="manifest.id"
          :data-testid="`manifest-${manifest.id}`"
          :data-status="manifest.状态"
          :class="{ selected: selected?.id === manifest.id }"
          @click="select(manifest.id)"
        >
          <td>{{ manifest.清单编号 }}</td>
          <td>{{ manifest.目标表 }}</td>
          <td>{{ manifest.源批次 }}</td>
          <td><code>{{ manifest.规则版本 }}</code></td>
          <td><code>{{ manifest.模型结构版本 }}</code></td>
          <td>{{ manifest.已交付分区数 }}/{{ manifest.分区总数 }}</td>
          <td>{{ manifest.状态 }}</td>
          <td>{{ manifest.试用运行次数 }}</td>
          <td class="row-actions" @click.stop>
            <button class="link" type="button" :data-testid="`try-${manifest.id}`" @click="run(manifest.id, '试运行')">
              试运行
            </button>
            <button class="link" type="button" :data-testid="`publish-${manifest.id}`" @click="run(manifest.id, '正式')">
              正式交付
            </button>
            <button class="link" type="button" :data-testid="`recompute-${manifest.id}`" @click="recompute(manifest.id)">
              复算
            </button>
            <button class="link" type="button" :data-testid="`verify-${manifest.id}`" @click="verify(manifest.id)">
              核对
            </button>
          </td>
        </tr>
        <tr v-if="!manifests.length">
          <td colspan="9" class="empty-state">暂无交付清单，先选择规则和源批次生成</td>
        </tr>
      </tbody>
    </table>

    <section v-if="selected" class="panel" data-testid="manifest-detail">
      <h3>{{ selected.清单编号 }} · {{ selected.目标表 }}</h3>
      <p class="muted">
        规则 {{ selected.规则名称 }}（<code>{{ selected.规则版本 }}</code>）· 源批次
        {{ selected.源批次 }} · 结构版本 <code>{{ selected.模型结构版本 }}</code>
      </p>
      <p v-if="!selected.一致性.规则与结构匹配" class="notice notice-error" data-testid="consistency-error">
        转换规则与目标字段结构不匹配，错误字段：{{ (selected.一致性.错误字段 as string[]).join('、') }}
      </p>
      <table class="data-table">
        <thead>
          <tr>
            <th>分区</th><th>源记录</th><th>状态</th><th>错误字段</th>
            <th>沿用建议</th><th>首次失败</th><th>交付编号</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="part in selected.分区" :key="part.分区" :data-testid="`partition-${part.分区}`">
            <td>{{ part.分区 }}</td>
            <td class="mono">{{ part.源记录.join(', ') || '—' }}</td>
            <td>{{ part.状态 }}</td>
            <td>{{ part.错误字段.join('、') || '—' }}</td>
            <td>{{ part.沿用建议 }}<span class="muted">（{{ part.沿用依据 }}）</span></td>
            <td>{{ part.首次失败 ? part.首次失败.原因 : '—' }}</td>
            <td>{{ part.交付编号 || '—' }}</td>
          </tr>
        </tbody>
      </table>

      <div class="detail-actions">
        <button class="btn" data-testid="locate-failures" type="button" @click="locateFailures(selected.源批次)">
          按源批次定位失败分区
        </button>
        <button class="btn" data-testid="check-units" type="button" @click="checkUnits(selected.id)">
          分区单元对账
        </button>
        <label class="field inline">
          <span>对比旧清单</span>
          <select v-model.number="compareOldId" data-testid="compare-old">
            <option v-for="manifest in comparableManifests" :key="manifest.id" :value="manifest.id">
              {{ manifest.清单编号 }}（{{ manifest.模型结构版本 }}）
            </option>
          </select>
        </label>
        <button class="btn" data-testid="compare-run" type="button" :disabled="!compareOldId" @click="compare">
          对比新旧结构结果
        </button>
      </div>

      <pre v-if="detailOutput" class="output" data-testid="detail-output">{{ detailOutput }}</pre>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'

import {
  addTargetFields,
  checkPartitionUnits,
  compareManifests,
  createManifest,
  failuresByBatch,
  getManifest,
  listManifests,
  listRules,
  listSourceBatches,
  listTargets,
  recomputeManifest,
  runManifest,
  verifyManifest,
} from '@/api/delivery'
import type { Json, ManifestDetail, ManifestSummary, SourceBatch, TargetSummary } from '@/api/delivery'

const environments = ['开发', '预发', '生产', '灰度']
const busy = ref(false)
const message = ref('')
const messageError = ref(false)
const manifests = ref<ManifestSummary[]>([])
const batches = ref<SourceBatch[]>([])
const targets = ref<TargetSummary[]>([])
const rules = ref<Json[]>([])
const selected = ref<ManifestDetail | null>(null)
const detailOutput = ref('')
const compareOldId = ref<number | null>(null)

const form = reactive({ rule_id: 0, source_batch: '', environment: '预发' })
const schemaForm = reactive({ table: '', fields: '', required: '' })

const stats = computed(() => [
  { label: '交付清单', value: manifests.value.length },
  { label: '已交付分区', value: manifests.value.reduce((sum, item) => sum + item.已交付分区数, 0) },
  {
    label: '未交付分区',
    value: manifests.value.reduce((sum, item) => sum + (item.分区总数 - item.已交付分区数), 0),
  },
  { label: '结构不兼容清单', value: manifests.value.filter((item) => item.状态 === '结构不兼容').length },
])

const comparableManifests = computed(() =>
  manifests.value.filter(
    (item) => selected.value && item.源批次 === selected.value.源批次 && item.id !== selected.value.id,
  ),
)

function say(text: string, isError = false) {
  message.value = text
  messageError.value = isError
}

async function reloadAll() {
  const [manifestPayload, batchPayload, targetPayload, rulePayload] = await Promise.all([
    listManifests(),
    listSourceBatches(),
    listTargets(),
    listRules(),
  ])
  manifests.value = manifestPayload.items
  batches.value = batchPayload.items
  targets.value = targetPayload.items
  rules.value = rulePayload.items
  if (!form.rule_id && rules.value.length) form.rule_id = Number(rules.value[0].id)
  if (!form.source_batch && batches.value.length) form.source_batch = batches.value[0].源批次
  if (!schemaForm.table && targets.value.length) schemaForm.table = targets.value[0].表名
  if (selected.value) {
    try {
      selected.value = await getManifest(selected.value.id)
    } catch {
      selected.value = null
    }
  }
}

async function submitManifest() {
  busy.value = true
  try {
    const result = await createManifest({
      rule_id: form.rule_id,
      source_batch: form.source_batch,
      environment: form.environment,
    })
    say(result.message)
    await reloadAll()
    await select(result.manifest.id)
  } catch (error) {
    say(error instanceof Error ? error.message : '生成失败', true)
  } finally {
    busy.value = false
  }
}

async function submitSchema() {
  busy.value = true
  try {
    const fields = schemaForm.fields
      .split(/[,，\s]+/)
      .map((item) => item.trim())
      .filter(Boolean)
      .map((field) => ({ 字段: field }))
    const required = schemaForm.required
      .split(/[,，\s]+/)
      .map((item) => item.trim())
      .filter(Boolean)
    const result = await addTargetFields(schemaForm.table, fields, required.length ? required : undefined)
    say(result.message)
    detailOutput.value = ''
    await reloadAll()
  } catch (error) {
    say(error instanceof Error ? error.message : '结构更新失败', true)
  } finally {
    busy.value = false
  }
}

async function select(id: number) {
  try {
    selected.value = await getManifest(id)
    detailOutput.value = ''
    compareOldId.value = null
  } catch (error) {
    say(error instanceof Error ? error.message : '清单读取失败', true)
  }
}

async function run(id: number, mode: '正式' | '试运行') {
  try {
    const response = await runManifest(id, mode)
    say(response.message, !response.ok)
    detailOutput.value = JSON.stringify(response.result, null, 2)
    await reloadAll()
    await select(id)
  } catch (error) {
    say(error instanceof Error ? error.message : '执行失败', true)
  }
}

async function recompute(id: number) {
  try {
    const result = await recomputeManifest(id)
    detailOutput.value = JSON.stringify(result, null, 2)
    say(`复算完成：${result.一致分区数}/${result.可复算分区数} 个已确认分区一致`)
  } catch (error) {
    say(error instanceof Error ? error.message : '复算失败', true)
  }
}

async function verify(id: number) {
  try {
    const result = await verifyManifest(id)
    detailOutput.value = JSON.stringify(result, null, 2)
    say(
      result.通过 ? '核对通过：构建记录、流水线分区与目标字段一致' : '核对未通过，请查看核对明细',
      !result.通过,
    )
  } catch (error) {
    say(error instanceof Error ? error.message : '核对失败', true)
  }
}

async function locateFailures(batch: string) {
  try {
    const result = await failuresByBatch(batch)
    detailOutput.value = JSON.stringify(result, null, 2)
    say(
      result.可定位
        ? `源批次 ${batch} 可定位 ${result.失败分区数} 个失败分区`
        : `源批次 ${batch} 没有失败分区`,
    )
  } catch (error) {
    say(error instanceof Error ? error.message : '失败分区定位失败', true)
  }
}

async function checkUnits(id: number) {
  try {
    const result = await checkPartitionUnits(id)
    detailOutput.value = JSON.stringify(result, null, 2)
    const blocked = result.被拦下分区 as string[]
    say(
      blocked.length
        ? `分区单元对账：${blocked.length} 个分区结构不兼容，未交付`
        : '分区单元对账：全部分区结构可用于正式交付',
      blocked.length > 0,
    )
  } catch (error) {
    say(error instanceof Error ? error.message : '分区单元对账失败', true)
  }
}

async function compare() {
  if (!selected.value || !compareOldId.value) return
  try {
    const result = await compareManifests(compareOldId.value, selected.value.id)
    detailOutput.value = JSON.stringify(result, null, 2)
    say(`对比完成：${result.差异分区数} 个分区存在差异`)
  } catch (error) {
    say(error instanceof Error ? error.message : '对比失败', true)
  }
}

onMounted(async () => {
  try {
    await reloadAll()
    // 从部署流水线跳转过来时带上 ?manifest=<id>，直接展开对应清单。
    const manifestId = Number(new URLSearchParams(window.location.search).get('manifest'))
    if (manifestId) await select(manifestId)
  } catch (error) {
    say(error instanceof Error ? error.message : '交付清单读取失败', true)
  }
})
</script>
