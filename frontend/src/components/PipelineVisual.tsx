import type { LayerStats } from '../api'

export const LAYER_COPY: Record<string, string> = {
  raw: 'Original CSV landings, untouched.',
  bronze: 'Cleaned columns, stored as Parquet.',
  silver: 'Typed, filtered, business-ready rows.',
  gold: 'Category aggregates for reporting.',
}

export function formatMoney(value: number) {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 2,
  }).format(value)
}

export function PipelineVisual({ layers }: { layers: LayerStats[] }) {
  const byId = Object.fromEntries(layers.map((layer) => [layer.id, layer]))
  const nodes = ['raw', 'bronze', 'silver', 'gold'] as const

  return (
    <svg className="pipeline-svg" viewBox="0 0 1000 180" role="img" aria-label="Lakehouse pipeline flow">
      <path
        className="flow-line"
        d="M120 90 H280 M320 90 H480 M520 90 H680 M720 90 H880"
      />
      {nodes.map((id, index) => {
        const x = 80 + index * 220
        const layer = byId[id]
        const ready = layer?.exists ?? false
        return (
          <g key={id} transform={`translate(${x}, 40)`}>
            <rect className={`node${ready ? ' ready' : ''}`} width="160" height="100" rx="16" />
            <circle className="pulse-dot" cx="20" cy="22" r="4" style={{ animationDelay: `${index * 0.25}s` }} />
            <text className="node-label" x="36" y="28">
              {layer?.label ?? id}
            </text>
            <text className="node-meta" x="20" y="58">
              {ready ? `${layer.rows} rows` : 'missing'}
            </text>
            <text className="node-meta" x="20" y="78">
              {layer?.format?.toUpperCase() ?? '—'}
            </text>
          </g>
        )
      })}
    </svg>
  )
}
