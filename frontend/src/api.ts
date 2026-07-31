export type LayerStats = {
  id: string
  label: string
  exists: boolean
  rows: number
  columns: string[]
  updated_at: string | null
  format: string
}

export type ServiceStatus = {
  id: string
  label: string
  ok: boolean
  required_for_demos: boolean
  detail: string
  hint: string
  url?: string
  missing?: string[]
  manifest_present?: boolean
}

export type StackStatus = {
  checked_at: string
  demos_ready: boolean
  services: ServiceStatus[]
  summary: string
}

export type Overview = {
  project: string
  stack: string[]
  ready: boolean
  demos_ready?: boolean
  layers: LayerStats[]
  services?: ServiceStatus[]
  summary?: string
  checked_at?: string
}

export type ScenarioOverview = {
  slug: string
  industry: string
  entity: string
  group_label: string
  value_label: string
  ready: boolean
  layers: LayerStats[]
}

export type GoldCategory = {
  category: string
  transaction_count: number
  total_value: number
  average_value: number
  latest_order_date?: string
  latest_event_date?: string
}

export type GoldResponse = {
  categories: GoldCategory[]
  totals: {
    total_value: number
    transaction_count: number
    category_count: number
  }
}

export type OrdersResponse = {
  layer: string
  columns: string[]
  rows: Record<string, string | number | null>[]
  returned: number
}

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(path)
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`)
  }
  return response.json() as Promise<T>
}

export const api = {
  overview: () => getJson<Overview>('/api/overview'),
  status: () => getJson<StackStatus>('/api/status'),
  gold: () => getJson<GoldResponse>('/api/gold'),
  orders: (layer: 'raw' | 'bronze' | 'silver') =>
    getJson<OrdersResponse>(`/api/orders?layer=${layer}`),
  scenario: (slug: string) => getJson<ScenarioOverview>(`/api/scenarios/${slug}`),
  scenarioGold: (slug: string) => getJson<GoldResponse>(`/api/scenarios/${slug}/gold`),
  scenarioOrders: (slug: string, layer: 'raw' | 'bronze' | 'silver') =>
    getJson<OrdersResponse>(`/api/scenarios/${slug}/orders?layer=${layer}`),
}
