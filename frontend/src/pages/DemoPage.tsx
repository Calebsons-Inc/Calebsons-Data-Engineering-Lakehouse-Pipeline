import { useEffect, useState } from 'react'
import { Link, Navigate, useParams } from 'react-router-dom'
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import {
  api,
  type GoldResponse,
  type ScenarioOverview,
  type StackStatus,
} from '../api'
import { LAYER_COPY, PipelineVisual, formatMoney } from '../components/PipelineVisual'
import { USE_CASES, getUseCase } from '../useCases'

export default function DemoPage() {
  const { slug = '' } = useParams()
  const useCase = getUseCase(slug)

  const [overview, setOverview] = useState<ScenarioOverview | null>(null)
  const [status, setStatus] = useState<StackStatus | null>(null)
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
        const statusData = await api.status()
        if (cancelled) return
        setStatus(statusData)

        if (!statusData.demos_ready) {
          setError(null)
          return
        }

        const [overviewData, goldData] = await Promise.all([
          api.scenario(slug),
          api.scenarioGold(slug),
        ])
        if (cancelled) return
        setOverview(overviewData)
        setGold(goldData)
        setError(null)
      } catch {
        if (!cancelled) {
          setError(
            'Could not load this industry scenario. Seed data with python transformations/seed_scenarios.py',
          )
        }
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    void load()
    return () => {
      cancelled = true
    }
  }, [slug])

  useEffect(() => {
    let cancelled = false

    async function loadOrders() {
      try {
        const data = await api.scenarioOrders(slug, layer)
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

    if (status?.demos_ready) {
      void loadOrders()
    }

    return () => {
      cancelled = true
    }
  }, [layer, status?.demos_ready, slug])

  if (!useCase) {
    return <Navigate to="/" replace />
  }

  const nextCases = USE_CASES.filter((item) => item.slug !== useCase.slug).slice(0, 2)
  const demosReady = Boolean(status?.demos_ready)
  const groupNoun = overview?.group_label ?? useCase.groupNoun
  const valueNoun = overview?.value_label ?? useCase.valueNoun
  const entity = overview?.entity ?? 'records'

  if (!loading && status && !demosReady) {
    return (
      <div className="page">
        <header className="page-header">
          <div>
            <p className="section-kicker">Demo locked</p>
            <h1>Stack not ready yet</h1>
            <p className="lede">{status.summary}</p>
          </div>
          <div className="page-actions">
            <Link className="btn primary" to="/">
              Back to overview
            </Link>
          </div>
        </header>
        <div className="panel">
          <ul className="service-list">
            {status.services.map((service) => (
              <li key={service.id} className={service.ok ? 'ok' : 'down'}>
                <div className="service-top">
                  <strong>{service.label}</strong>
                  <span>{service.ok ? 'Ready' : 'Not ready'}</span>
                </div>
                <p>{service.detail}</p>
                {!service.ok && <code>{service.hint}</code>}
              </li>
            ))}
          </ul>
        </div>
      </div>
    )
  }

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <p className="section-kicker">
            {useCase.industry} · {useCase.audience}
          </p>
          <h1>{useCase.headline}</h1>
          <p className="lede">{useCase.lede}</p>
        </div>
      </header>

      <div className="dashboard-grid">
        <section className="panel span-12" aria-labelledby="layers-title">
          <div className="panel-head">
            <div>
              <p className="section-kicker">Medallion layers</p>
              <h2 id="layers-title">Raw → bronze → silver → gold</h2>
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
              <h2 id="gold-title">
                {groupNoun} {valueNoun.toLowerCase()}
              </h2>
            </div>
          </div>
          <div className="metric-row stacked">
            <div className="metric">
              <span>Total {valueNoun.toLowerCase()}</span>
              <strong>{gold ? formatMoney(gold.totals.total_value) : '—'}</strong>
            </div>
            <div className="metric">
              <span>{entity}</span>
              <strong>{gold?.totals.transaction_count ?? '—'}</strong>
            </div>
            <div className="metric">
              <span>{groupNoun}s</span>
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
              <p className="section-kicker">Tables</p>
              <h2 id="explorer-title">Inspect by layer</h2>
            </div>
            <div className="explorer-controls">
              <label htmlFor="demo-layer-select">Layer</label>
              <select
                id="demo-layer-select"
                value={layer}
                onChange={(event) =>
                  setLayer(event.target.value as 'raw' | 'bronze' | 'silver')
                }
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

        <section className="panel span-12" aria-labelledby="rollup-title">
          <div className="panel-head">
            <div>
              <p className="section-kicker">Gold detail</p>
              <h2 id="rollup-title">
                {valueNoun} by {groupNoun.toLowerCase()}
              </h2>
            </div>
          </div>
          <div className="rollup-list gridish">
            {(gold?.categories ?? []).map((category) => (
              <article key={category.category} className="rollup-item">
                <h3>{category.category}</h3>
                <p>
                  {category.transaction_count} {entity} · avg{' '}
                  {formatMoney(category.average_value)}
                </p>
                <strong>{formatMoney(category.total_value)}</strong>
              </article>
            ))}
          </div>
        </section>

        <section className="panel span-12 more-demos" aria-labelledby="more-title">
          <div className="panel-head">
            <div>
              <p className="section-kicker">Next</p>
              <h2 id="more-title">Try another industry</h2>
            </div>
          </div>
          <div className="demo-grid compact">
            {nextCases.map((item) => (
              <Link key={item.slug} className="demo-link" to={`/demos/${item.slug}`}>
                <span className="demo-audience">{item.industry}</span>
                <strong>{item.title}</strong>
                <span>{item.short}</span>
              </Link>
            ))}
            <Link className="demo-link" to="/lakehouse">
              <span className="demo-audience">Core pipeline</span>
              <strong>Lakehouse explorer</strong>
              <span>Open the shared sales pipeline, gold chart, and row browser.</span>
            </Link>
          </div>
        </section>
      </div>

      {error && <p className="status error">{error}</p>}
      {loading && <p className="status">Loading industry demo…</p>}
    </div>
  )
}
