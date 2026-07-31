import type { ServiceStatus, StackStatus } from '../api'

type Props = {
  status: StackStatus | null
  onRefresh: () => void
  refreshing?: boolean
  compact?: boolean
}

export function StackStatusBoard({
  status,
  onRefresh,
  refreshing = false,
  compact = false,
}: Props) {
  const services: ServiceStatus[] = status?.services ?? []

  return (
    <section
      id="status"
      className={`status-board${compact ? ' compact' : ''}`}
      aria-labelledby="status-title"
    >
      <div className="status-board-head">
        <div>
          <p className="section-kicker">Stack readiness</p>
          <h2 id="status-title">{compact ? 'Services' : 'Unlock demos when the stack is green'}</h2>
          {!compact && (
            <p className="section-copy">
              {status?.summary ??
                'Backend and frontend can run alone. Airflow, dbt, warehouse, and layers must be ready before demos open.'}
            </p>
          )}
        </div>
        <button type="button" className="btn ghost" onClick={onRefresh} disabled={refreshing}>
          {refreshing ? 'Checking…' : 'Recheck'}
        </button>
      </div>

      <ul className="service-list">
        {services.map((service) => (
          <li key={service.id} className={service.ok ? 'ok' : 'down'}>
            <div className="service-top">
              <strong>{service.label}</strong>
              <span>{service.ok ? 'Ready' : 'Down'}</span>
            </div>
            {!compact && <p>{service.detail}</p>}
            {!service.ok && !compact && <code>{service.hint}</code>}
          </li>
        ))}
      </ul>
      {compact && status?.summary && <p className="status-summary">{status.summary}</p>}
    </section>
  )
}
