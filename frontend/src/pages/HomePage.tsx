import { useCallback, useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { api, type Overview, type StackStatus } from '../api'
import { PipelineVisual } from '../components/PipelineVisual'
import { StackStatusBoard } from '../components/StackStatusBoard'
import { USE_CASES } from '../useCases'

export default function HomePage() {
  const [overview, setOverview] = useState<Overview | null>(null)
  const [status, setStatus] = useState<StackStatus | null>(null)
  const [refreshing, setRefreshing] = useState(false)
  const [apiDown, setApiDown] = useState(false)

  const refresh = useCallback(async () => {
    setRefreshing(true)
    try {
      const [overviewData, statusData] = await Promise.all([api.overview(), api.status()])
      setOverview(overviewData)
      setStatus(statusData)
      setApiDown(false)
    } catch {
      setApiDown(true)
      setStatus(null)
    } finally {
      setRefreshing(false)
    }
  }, [])

  useEffect(() => {
    void refresh()
    const timer = window.setInterval(() => {
      void refresh()
    }, 10000)
    return () => window.clearInterval(timer)
  }, [refresh])

  const demosReady = Boolean(status?.demos_ready)
  const servicesOk = status?.services.filter((item) => item.ok).length ?? 0
  const servicesTotal = status?.services.length ?? 0
  const layersReady = overview?.layers.filter((item) => item.exists).length ?? 0

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <p className="section-kicker">Overview</p>
          <h1>Lakehouse dashboard</h1>
          <p className="lede">
            Run the API and UI first. When Airflow, dbt, and layers are green, demos unlock.
          </p>
        </div>
        <div className="page-actions">
          <button type="button" className="btn ghost" onClick={() => void refresh()} disabled={refreshing}>
            {refreshing ? 'Refreshing…' : 'Refresh'}
          </button>
          <Link className="btn primary" to="/lakehouse">
            Open explorer
          </Link>
        </div>
      </header>

      {apiDown && (
        <p className="banner error">
          Backend is down. Start with{' '}
          <code>.venv-ui/bin/python -m uvicorn api.server:app --reload --port 8000</code>
        </p>
      )}

      <section className="kpi-grid" aria-label="Key metrics">
        <article className="kpi">
          <span>Demos</span>
          <strong className={demosReady ? 'good' : 'warn'}>{demosReady ? 'Unlocked' : 'Locked'}</strong>
        </article>
        <article className="kpi">
          <span>Services ready</span>
          <strong>
            {servicesOk}/{servicesTotal || '—'}
          </strong>
        </article>
        <article className="kpi">
          <span>Layers online</span>
          <strong>
            {layersReady}/{overview?.layers.length ?? 4}
          </strong>
        </article>
        <article className="kpi">
          <span>Stack</span>
          <strong className="stack-pill">{(overview?.stack ?? ['Polars', 'DuckDB', 'Airflow', 'dbt']).join(' · ')}</strong>
        </article>
      </section>

      <div className="dashboard-grid">
        <div className="panel span-8">
          <div className="panel-head">
            <div>
              <p className="section-kicker">Pipeline</p>
              <h2>Medallion flow</h2>
            </div>
          </div>
          <div className="hero-visual">
            <PipelineVisual layers={overview?.layers ?? []} />
          </div>
          <div className="layer-rail compact">
            {(overview?.layers ?? []).map((item) => (
              <article
                key={item.id}
                className={`layer-step${item.exists ? ' ready' : ''}`}
                data-layer={item.id}
              >
                <h3>{item.label}</h3>
                <div className="count">{item.exists ? item.rows : '—'}</div>
              </article>
            ))}
          </div>
        </div>

        <div className="panel span-4">
          <StackStatusBoard status={status} onRefresh={() => void refresh()} refreshing={refreshing} compact />
        </div>

        <section className="panel span-12" id="demos" aria-labelledby="demos-title">
          <div className="panel-head">
            <div>
              <p className="section-kicker">Use cases</p>
              <h2 id="demos-title">Industry demos</h2>
              <p className="section-copy">
                {demosReady
                  ? 'Each demo uses its own dataset from a different industry.'
                  : 'Demos stay locked until every required service is ready.'}
              </p>
            </div>
          </div>
          <div className="demo-grid">
            {USE_CASES.map((useCase) =>
                demosReady ? (
                  <Link key={useCase.slug} className="demo-link" to={`/demos/${useCase.slug}`}>
                    <span className="demo-audience">{useCase.industry} · {useCase.audience}</span>
                    <strong>{useCase.title}</strong>
                    <span>{useCase.short}</span>
                  </Link>
                ) : (
                  <div key={useCase.slug} className="demo-link locked" aria-disabled="true">
                    <span className="demo-audience">{useCase.industry} · {useCase.audience}</span>
                    <strong>{useCase.title}</strong>
                    <span>{useCase.short}</span>
                    <em className="lock-note">Locked — waiting on stack</em>
                  </div>
                ),
            )}
          </div>
        </section>
      </div>
    </div>
  )
}
