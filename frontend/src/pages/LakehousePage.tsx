import { useEffect, useState } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { api, type GoldResponse, type Overview } from '../api'
import { LAYER_COPY, PipelineVisual, formatMoney } from '../components/PipelineVisual'

export default function LakehousePage() {
  const [overview, setOverview] = useState<Overview | null>(null)
  const [gold, setGold] = useState<GoldResponse | null>(null)
  const [layer, setLayer] = useState<'raw' | 'bronze' | 'silver'>('silver')
  const [orders, setOrders] = useState<Record<string, string | number | null>[]>([])
  const [columns, setColumns] = useState<string[]>([])
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let cancelled = false

    async function load() {
      try {
        setLoading(true)
        const [overviewData, goldData] = await Promise.all([api.overview(), api.gold()])
        if (cancelled) return
        setOverview(overviewData)
        setGold(goldData)
        setError(null)
      } catch {
        if (!cancelled) {
          setError('Could not reach the lakehouse API. Start it with uvicorn on port 8000.')
        }
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    void load()
    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    let cancelled = false

    async function loadOrders() {
      try {
        const data = await api.orders(layer)
        if (cancelled) return
        setColumns(data.columns)
        setOrders(data.rows)
      } catch {
        if (!cancelled) {
          setOrders([])
          setColumns([])
        }
      }
    }

    void loadOrders()
    return () => {
      cancelled = true
    }
  }, [layer])

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <p className="section-kicker">Explorer</p>
          <h1>Lakehouse layers</h1>
          <p className="lede">
            Inspect every layer, gold aggregates, and live order rows from the same pipeline.
          </p>
        </div>
      </header>

      <div className="dashboard-grid">
        <section className="panel span-12">
          <div className="panel-head">
            <div>
              <p className="section-kicker">Pipeline</p>
              <h2>Four layers. One path.</h2>
            </div>
          </div>
          <div className="hero-visual">
            <PipelineVisual layers={overview?.layers ?? []} />
          </div>
          <div className="layer-rail">
            {(overview?.layers ?? []).map((item) => (
              <article
                key={item.id}
                className={`layer-step${item.exists ? ' ready' : ''}`}
                data-layer={item.id}
              >
                <h3>{item.label}</h3>
                <p>{LAYER_COPY[item.id]}</p>
                <div className="count">{item.exists ? item.rows : '—'}</div>
              </article>
            ))}
          </div>
        </section>

        <section className="panel span-5" aria-labelledby="gold-title">
          <div className="panel-head">
            <div>
              <p className="section-kicker">Gold</p>
              <h2 id="gold-title">Category value</h2>
            </div>
          </div>
          <div className="metric-row stacked">
            <div className="metric">
              <span>Total value</span>
              <strong>{gold ? formatMoney(gold.totals.total_value) : '—'}</strong>
            </div>
            <div className="metric">
              <span>Transactions</span>
              <strong>{gold?.totals.transaction_count ?? '—'}</strong>
            </div>
            <div className="metric">
              <span>Categories</span>
              <strong>{gold?.totals.category_count ?? '—'}</strong>
            </div>
          </div>
          <div className="chart-wrap">
            {gold && gold.categories.length > 0 ? (
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={gold.categories} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
                  <CartesianGrid stroke="rgba(19,38,31,0.08)" vertical={false} />
                  <XAxis dataKey="category" stroke="#5d7469" tickLine={false} axisLine={false} />
                  <YAxis stroke="#5d7469" tickLine={false} axisLine={false} />
                  <Tooltip
                    cursor={{ fill: 'rgba(184,137,31,0.08)' }}
                    contentStyle={{
                      background: '#ffffff',
                      border: '1px solid rgba(19,38,31,0.12)',
                      borderRadius: 10,
                      color: '#13261f',
                    }}
                    formatter={(value) => formatMoney(Number(value ?? 0))}
                  />
                  <Bar dataKey="total_value" fill="#b8891f" radius={[8, 8, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <p className="status">No gold aggregates yet.</p>
            )}
          </div>
        </section>

        <section className="panel span-7" aria-labelledby="explorer-title">
          <div className="panel-head">
            <div>
              <p className="section-kicker">Rows</p>
              <h2 id="explorer-title">Inspect by layer</h2>
            </div>
            <div className="explorer-controls">
              <label htmlFor="layer-select">Layer</label>
              <select
                id="layer-select"
                value={layer}
                onChange={(event) => setLayer(event.target.value as 'raw' | 'bronze' | 'silver')}
              >
                <option value="raw">Raw</option>
                <option value="bronze">Bronze</option>
                <option value="silver">Silver</option>
              </select>
            </div>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  {columns.map((column) => (
                    <th key={column}>{column}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {orders.map((row, index) => (
                  <tr key={`${layer}-${index}`}>
                    {columns.map((column) => (
                      <td key={column}>{row[column] ?? '—'}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      </div>

      {loading && <p className="status">Loading lakehouse…</p>}
      {error && <p className="status error">{error}</p>}
    </div>
  )
}
