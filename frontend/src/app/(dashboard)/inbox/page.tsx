import Link from 'next/link'
import { redirect } from 'next/navigation'
import { createClient } from '@/lib/supabase/server'

// Reason badge styling map
const reasonBadges: Record<string, { label: string; className: string }> = {
  math_variance: {
    label: 'Math Variance',
    className: 'bg-orange-100 text-orange-800',
  },
  missing_claim_id: {
    label: 'Missing Claim ID',
    className: 'bg-red-100 text-red-800',
  },
  no_check_total: {
    label: 'No Check Total',
    className: 'bg-yellow-100 text-yellow-800',
  },
  partial_failure: {
    label: 'Partial Failure',
    className: 'bg-red-100 text-red-800',
  },
  low_confidence: {
    label: 'Low Confidence',
    className: 'bg-purple-100 text-purple-800',
  },
}

export default async function InboxPage() {
  const supabase = await createClient()

  const {
    data: { user },
  } = await supabase.auth.getUser()

  if (!user) redirect('/login')

  const { data: practiceLink } = await supabase
    .from('practice_users')
    .select('practice_id')
    .eq('user_id', user.id)
    .limit(1)
    .single()

  if (!practiceLink) redirect('/setup')

  // Fetch documents needing review
  const { data: documents } = await supabase
    .from('eob_documents')
    .select(
      'id, file_name, status, total_pages, items_extracted, created_at, review_status, review_reasons, has_found_revenue'
    )
    .eq('practice_id', practiceLink.practice_id)
    .eq('review_status', 'needs_review')
    .order('created_at', { ascending: false })

  return (
    <div>
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Inbox</h1>
          <p className="mt-1 text-sm text-gray-500">
            Documents flagged for review. Fix exceptions, then re-generate the 835.
          </p>
        </div>
        {documents && documents.length > 0 && (
          <span className="inline-flex items-center rounded-full bg-red-50 px-3 py-1 text-sm font-semibold text-red-700 ring-1 ring-red-600/20 ring-inset">
            {documents.length} document{documents.length !== 1 ? 's' : ''} need
            review
          </span>
        )}
      </div>

      <div className="mt-6 overflow-hidden rounded-xl border border-gray-200 bg-white shadow-sm">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
                File Name
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
                Status
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
                Review Reasons
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
                Items
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
                Uploaded
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500"></th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-200">
            {documents?.map((doc) => {
              const reasons = (doc.review_reasons as string[]) || []
              return (
                <tr key={doc.id} className="hover:bg-gray-50">
                  <td className="px-6 py-4">
                    <div className="flex items-center gap-2">
                      <span className="text-sm font-medium text-gray-900 max-w-xs truncate">
                        {doc.file_name || doc.id.substring(0, 8)}
                      </span>
                      {doc.has_found_revenue && (
                        <span className="inline-flex items-center rounded-full bg-green-100 px-2 py-0.5 text-xs font-semibold text-green-800 ring-1 ring-green-600/20 ring-inset">
                          Found Revenue
                        </span>
                      )}
                    </div>
                  </td>
                  <td className="px-6 py-4">
                    <span
                      className={`inline-flex rounded-full px-2 py-1 text-xs font-semibold ${
                        doc.status === 'completed'
                          ? 'bg-green-100 text-green-800'
                          : doc.status === 'partial_failure'
                            ? 'bg-red-100 text-red-800'
                            : 'bg-gray-100 text-gray-800'
                      }`}
                    >
                      {doc.status === 'partial_failure'
                        ? 'Partial Failure'
                        : doc.status}
                    </span>
                  </td>
                  <td className="px-6 py-4">
                    <div className="flex flex-wrap gap-1">
                      {reasons.map((reason) => {
                        const badge = reasonBadges[reason] || {
                          label: reason,
                          className: 'bg-gray-100 text-gray-800',
                        }
                        return (
                          <span
                            key={reason}
                            className={`inline-flex rounded-full px-2 py-0.5 text-xs font-semibold ${badge.className}`}
                          >
                            {badge.label}
                          </span>
                        )
                      })}
                    </div>
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-500">
                    {doc.items_extracted ?? 0}
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-500">
                    {new Date(doc.created_at).toLocaleDateString()}
                  </td>
                  <td className="px-6 py-4 text-sm">
                    <Link
                      href={`/documents/${doc.id}`}
                      className="inline-flex items-center rounded-md bg-blue-600 px-3 py-1.5 text-xs font-semibold text-white shadow-sm hover:bg-blue-500"
                    >
                      Review
                    </Link>
                  </td>
                </tr>
              )
            })}
            {(!documents || documents.length === 0) && (
              <tr>
                <td
                  colSpan={6}
                  className="px-6 py-12 text-center"
                >
                  <svg
                    className="mx-auto h-10 w-10 text-green-400"
                    fill="none"
                    viewBox="0 0 24 24"
                    strokeWidth="1.5"
                    stroke="currentColor"
                  >
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      d="M9 12.75L11.25 15 15 9.75M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
                    />
                  </svg>
                  <h3 className="mt-2 text-sm font-semibold text-gray-900">
                    All clear!
                  </h3>
                  <p className="mt-1 text-sm text-gray-500">
                    No documents need review right now.
                  </p>
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
