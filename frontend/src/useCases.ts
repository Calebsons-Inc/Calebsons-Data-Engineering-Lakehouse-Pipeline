export type UseCase = {
  slug: string
  title: string
  industry: string
  short: string
  headline: string
  lede: string
  audience: string
  groupNoun: string
  valueNoun: string
}

export const USE_CASES: UseCase[] = [
  {
    slug: 'retail-pos-reporting',
    title: 'Retail POS reporting',
    industry: 'Retail',
    short: 'Point-of-sale tickets become trusted category revenue for store finance.',
    headline: 'Publish store category revenue finance can trust.',
    lede: 'Nightly POS exports from grocery, apparel, and electronics lanes land as CSV. The lakehouse cleans them and publishes gold category totals.',
    audience: 'Retail finance',
    groupNoun: 'Category',
    valueNoun: 'Sales',
  },
  {
    slug: 'clinic-appointment-ops',
    title: 'Clinic appointment ops',
    industry: 'Healthcare',
    short: 'Clinic appointments become trusted department fee totals after cleaning.',
    headline: 'Spot bad appointments before they hit clinical reports.',
    lede: 'After a scheduling system change, clinic appointments land as CSV. Compare raw → silver → gold to find cancellations, no-shows, and invalid fees.',
    audience: 'Clinic ops',
    groupNoun: 'Department',
    valueNoun: 'Fees',
  },
  {
    slug: 'fleet-delivery-rollups',
    title: 'Fleet delivery rollups',
    industry: 'Logistics',
    short: 'Hub shipment files become one trusted freight summary for leadership.',
    headline: 'One freight summary instead of hub spreadsheets.',
    lede: 'Each regional hub drops daily shipment files. The lakehouse cleans them and gold aggregates delivered freight by hub.',
    audience: 'Network leadership',
    groupNoun: 'Hub',
    valueNoun: 'Freight',
  },
  {
    slug: 'payments-quality-gate',
    title: 'Payments quality gate',
    industry: 'Fintech',
    short: 'Card payment events become settled channel volumes ready for BI.',
    headline: 'Only promote settled payments when counts look sane.',
    lede: 'Risk and BI inspect every layer on card payments, then unlock Looker on settled channel volumes from gold.',
    audience: 'Risk & BI',
    groupNoun: 'Channel',
    valueNoun: 'Volume',
  },
  {
    slug: 'saas-usage-onboarding',
    title: 'SaaS usage onboarding',
    industry: 'SaaS',
    short: 'Walk medallion layers on subscription seat-value events.',
    headline: 'Learn the lakehouse on real SaaS usage events.',
    lede: 'New engineers walk raw → bronze → silver → gold on product seat-value events, then trigger Airflow on a working local stack.',
    audience: 'Engineering',
    groupNoun: 'Product',
    valueNoun: 'Seat value',
  },
]

export function getUseCase(slug: string): UseCase | undefined {
  return USE_CASES.find((item) => item.slug === slug)
}
