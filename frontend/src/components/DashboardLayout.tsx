import { useEffect, useState } from 'react'
import { NavLink, Outlet, useLocation } from 'react-router-dom'
import { USE_CASES } from '../useCases'

const NAV = [
  { to: '/', label: 'Overview', end: true },
  { to: '/lakehouse', label: 'Lakehouse', end: false },
  ...USE_CASES.map((item) => ({
    to: `/demos/${item.slug}`,
    label: item.title,
    end: false,
  })),
]

export function DashboardLayout() {
  const [open, setOpen] = useState(false)
  const location = useLocation()

  useEffect(() => {
    setOpen(false)
  }, [location.pathname])

  useEffect(() => {
    document.body.style.overflow = open ? 'hidden' : ''
    return () => {
      document.body.style.overflow = ''
    }
  }, [open])

  return (
    <div className="shell">
      <div className="atmosphere" aria-hidden="true" />

      <aside className={`sidebar${open ? ' open' : ''}`} id="app-sidebar" aria-label="Primary">
        <div className="sidebar-brand">
          <p className="brand-mark">
            Calebsons <span>Datalake</span>
          </p>
          <p className="brand-sub">Lakehouse console</p>
        </div>

        <nav className="sidebar-nav">
          <p className="nav-label">Workspace</p>
          {NAV.slice(0, 2).map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.end}
              className={({ isActive }) => `nav-item${isActive ? ' active' : ''}`}
            >
              {item.label}
            </NavLink>
          ))}

          <p className="nav-label">Demos</p>
          {NAV.slice(2).map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              className={({ isActive }) => `nav-item${isActive ? ' active' : ''}`}
            >
              {item.label}
            </NavLink>
          ))}
        </nav>
      </aside>

      {open && (
        <button
          type="button"
          className="sidebar-backdrop"
          aria-label="Close menu"
          onClick={() => setOpen(false)}
        />
      )}

      <div className="shell-main">
        <header className="topbar">
          <button
            type="button"
            className="menu-toggle"
            aria-expanded={open}
            aria-controls="app-sidebar"
            onClick={() => setOpen((value) => !value)}
          >
            <span />
            <span />
            <span />
            <span className="sr-only">Menu</span>
          </button>
          <div className="topbar-copy">
            <strong>Calebsons Datalake</strong>
            <span>Mobile-ready lakehouse dashboard</span>
          </div>
        </header>

        <div className="shell-content">
          <Outlet />
        </div>
      </div>
    </div>
  )
}
