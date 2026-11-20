/** ETL 分区交付清单接口：源批次、目标表结构、清单推进与核对。 */
import { request } from './client'

export type Json = Record<string, unknown>

export type ManifestSummary = {
  id: number
  清单编号: string
  目标表: string
  规则ID: number
  规则名称: string
  规则版本: string
  源批次: string
  模型结构版本: string
  部署环境: string
  状态: string
  分区总数: number
  已交付分区数: number
  失败分区数: number
  试用运行次数: number
  创建时间: string
}

export type ManifestPartition = {
  分区: string
  源批次: string
  源记录: string[]
  源记录数: number
  状态: string
  转换规则版本: string
  结构版本: string
  目标字段: string[]
  错误字段: string[]
  沿用建议: string
  沿用依据: string
  重试次数: number
  首次失败: Json | null
  交付编号: string
  交付时间: string
}

export type ManifestDetail = ManifestSummary & {
  目标字段: string[]
  必填字段: string[]
  字段结构: { 字段: string; 类型: string; 必填: boolean }[]
  一致性: Json
  分区: ManifestPartition[]
  构建记录: Json[]
  试运行记录: Json[]
  目标表实际可用字段: string[]
}

export type TargetSummary = {
  表名: string
  当前结构版本: string
  实际可用字段: string[]
  当前必填字段: string[]
  结构版本数: number
  已交付分区数: number
  分区状态: Record<string, number>
}

export type SourceBatch = {
  源批次: string
  源表: string
  目标表: string
  分区字段: string
  记录数: number
  分区: Record<string, number>
  状态: string
}

async function read<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await request(path, init)
  if (!response.ok) {
    let detail = `接口返回 ${response.status}`
    try {
      const body = (await response.json()) as { detail?: string }
      if (body.detail) detail = body.detail
    } catch {
      /* 忽略非 JSON 错误体 */
    }
    throw new Error(detail)
  }
  return (await response.json()) as T
}

function post<T>(path: string, body: unknown): Promise<T> {
  return read<T>(path, { method: 'POST', body: JSON.stringify(body) })
}

export function listManifests() {
  return read<{ items: ManifestSummary[]; total: number }>('/api/delivery/manifests')
}

export function getManifest(id: number) {
  return read<ManifestDetail>(`/api/delivery/manifests/${id}`)
}

export function listSourceBatches() {
  return read<{ items: SourceBatch[]; total: number }>('/api/delivery/source-batches')
}

export function listTargets() {
  return read<{ items: TargetSummary[]; total: number }>('/api/delivery/targets')
}

export type CreateManifestInput = {
  rule_id: number
  source_batch: string
  environment: string
  note?: string
}

export function createManifest(input: CreateManifestInput) {
  return post<{ ok: boolean; message: string; manifest: ManifestDetail }>(
    '/api/delivery/manifests',
    input,
  )
}

export function runManifest(id: number, mode: '正式' | '试运行', partitions: string[] = []) {
  return post<{ ok: boolean; message: string; result: Json }>(
    `/api/delivery/manifests/${id}/run`,
    { mode, partitions },
  )
}

export function recomputeManifest(id: number) {
  return read<Json>(`/api/delivery/manifests/${id}/recompute`)
}

export function verifyManifest(id: number) {
  return read<Json>(`/api/delivery/manifests/${id}/verify`)
}

export function checkPartitionUnits(id: number) {
  return read<Json>(`/api/delivery/manifests/${id}/check`)
}

export function failuresByBatch(sourceBatch: string) {
  return read<Json>(`/api/delivery/source-batches/${encodeURIComponent(sourceBatch)}/failures`)
}

export function compareManifests(oldId: number, newId: number) {
  return post<Json>('/api/delivery/compare', {
    old_manifest_id: oldId,
    new_manifest_id: newId,
  })
}

export function addTargetFields(
  table: string,
  fields: { 字段: string; 类型?: string; 必填?: boolean }[],
  requiredFields?: string[],
  note = '',
) {
  return post<{ ok: boolean; message: string; target: Json }>(
    `/api/delivery/targets/${encodeURIComponent(table)}/schema`,
    { add_fields: fields, required_fields: requiredFields, note },
  )
}

export function listRules() {
  return read<{ items: Json[]; total: number }>('/api/etl_mapping?size=200')
}
