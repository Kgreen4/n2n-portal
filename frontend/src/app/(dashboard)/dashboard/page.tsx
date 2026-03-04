import Link from 'next/link'
import { redirect } from 'next/navigation'
import { createClient } from '@/lib/supabase/server'

export default async function DashboardPage() {
  const supabase = await createClient()

  const {
    data: { user },
  } = await supabase.auth.getUser()

  if (!user) {
    redirect('/login')
  }

  // Fetch the user's practice link
  const { data: practiceLink } = await supabase
    .from('practice_users')
    .select('practice_id')
    .eq('user_id', user.id)
    .limit(1)
    .single()

  if (!practiceLink) {
    redirect('/setup')
  }

  const practiceId = practiceLink.practice_id

  // Fetch practice details, document count, and pipeline events in parallel
  const [practiceResult, docCountResult, eventsResult] = await Promise.all([
    supabase
      .from('practice_credits')
      .select('credits_remaining')
      .eq('practice_id', practiceId)
      .single(),
    supabase
      .from('eob_documents')
      .select('*', { count: 'exact', head: true })
      .eq('practice_id', practiceId),
    supabase
      .from('pipeline_events')
      .select('*')
      .eq('practice_id', practiceId)
      .order('created_at', { ascending: false })
      .limit(20),
  ])

  const credits = practiceResult.data?.credits_remaining ?? 0
  const docCount = docCountResult.count
  const events = eventsResult.data ?? []

  // Count duplicates in last 24 hours for alert banner
  const oneDayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString()
  const recentDuplicates = events.filter(
    (e: any) => e.event_type === 'duplicate_skipped' && e.created_at > oneDayAgo
  )
  const recentErrors = events.filter(
    (e: any) => e.event_type === 'processing_error' && e.created_at > oneDayAgo
  )

  return (
    <div>
      <h1 className="text-2xl font-bold text-gray-900">Dashboard</h1>
      <p className="mt-1 text-sm text-gray-500">Welcome back. Here is your practice overview.</p>

      <div className="mt-8 grid gap-6 sm:grid-cols-2 lg:grid-cols-3">
        {/* Credits Card */}
        <div className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
          <div className="flex items-center">
            <div className="flex h-12 w-12 items-center justify-center rounded-lg bg-blue-50">
              <svg className="h-6 w-6 text-blue-600" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" d="M2.25 18.75a60.07 60.07 0 0115.797 2.101c.727.198 1.453-.342 1.453-1.096V18.75M3.75 4.5v.75A.75.75 0 013 6h-.75m0 0v-.375c0-.621.504-1.125 1.125-1.125H20.25M2.25 6v9m18-10.5v.75c0 .414.336.75.75.75h.75m-1.5-1.5h.375c.621 0 1.125.504 1.125 1.125v9.75c0 .621-.504 1.125-1.125 1.125h-.375m1.5-1.5H21a.75.75 0 00-.75.75v.75m0 0H3.75m0 0h-.375a1.125 1.125 0 01-1.125-1.125V15m1.5 1.5v-.75A.75.75 0 003 15h-.75M15 10.5a3 3 0 11-6 0 3 3 0 016 0zm3 0h.008v.008H18V10.5zm-12 0h.008v.008H6V10.5z" />
              </svg>
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-500">Credits Remaining</p>
              <p className="text-3xl font-bold text-gray-900">{credits}</p>
            </div>
          </div>
          <p className="mt-4 text-xs text-gray-400">Each page of an EOB uses 1 credit</p>
        </div>

        {/* Documents Card */}
        <div className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
          <div className="flex items-center">
            <div className="flex h-12 w-12 items-center justify-center rounded-lg bg-green-50">
              <svg className="h-6 w-6 text-green-600" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m2.25 0H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z" />
              </svg>
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-500">Documents Processed</p>
              <p className="text-3xl font-bold text-gray-900">{docCount ?? 0}</p>
            </div>
          </div>
          <Link href="/documents" className="mt-4 inline-block text-xs font-medium text-blue-600 hover:text-blue-500">
            View all documents →
          </Link>
        </div>

        {/* Upload CTA Card */}
        <div className="rounded-xl border-2 border-dashed border-gray-300 bg-white p-6 shadow-sm flex flex-col items-center justify-center text-center">
          <svg className="h-10 w-10 text-gray-400" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5m-13.5-9L12 3m0 0l4.5 4.5M12 3v13.5" />
          </svg>
          <p className="mt-2 text-sm font-medium text-gray-900">Upload a new EOB</p>
          <p className="text-xs text-gray-500">PDF files up to 50 pages</p>
          <Link
            href="/upload"
            className="mt-4 inline-flex items-center rounded-md bg-blue-600 px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-blue-500"
          >
            Upload PDF
          </Link>
        </div>
      </div>

      {/* Alert Banners */}
      {recentDuplicates.length > 0 && (
        <div className="mt-6 rounded-lg border border-amber-200 bg-amber-50 p-4">
          <div className="flex items-center">
            <svg className="h-5 w-5 text-amber-500 mr-2 flex-shrink-0" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126zM12 15.75h.007v.008H12v-.008z" />
            </svg>
            <p className="text-sm font-medium text-amber-800">
              {recentDuplicates.length} duplicate file{recentDuplicates.length > 1 ? 's were' : ' was'} skipped by the folder watcher in the last 24 hours
            </p>
          </div>
        </div>
      )}
      {recentErrors.length > 0 && (
        <div className="mt-4 rounded-lg border border-red-200 bg-red-50 p-4">
          <div className="flex items-center">
            <svg className="h-5 w-5 text-red-500 mr-2 flex-shrink-0" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m9-.75a9 9 0 11-18 0 9 9 0 0118 0zm-9 3.75h.008v.008H12v-.008z" />
            </svg>
            <p className="text-sm font-medium text-red-800">
              {recentErrors.length} processing error{recentErrors.length > 1 ? 's' : ''} in the last 24 hours
            </p>
          </div>
        </div>
      )}

      {/* Pipeline Activity Feed */}
      <div className="mt-8">
        <h2 className="text-lg font-semibold text-gray-900">Recent Pipeline Activity</h2>
        <p className="mt-1 text-sm text-gray-500">Events from the folder watcher and manual uploads</p>

        {events.length === 0 ? (
          <div className="mt-4 rounded-xl border border-gray-200 bg-white p-8 text-center">
            <svg className="mx-auto h-10 w-10 text-gray-300" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" d="M12 6v6h4.5m4.5 0a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            <p className="mt-2 text-sm text-gray-500">No pipeline events yet. Upload an EOB or configure the folder watcher to get started.</p>
          </div>
        ) : (
          <div className="mt-4 overflow-hidden rounded-xl border border-gray-200 bg-white shadow-sm">
            <ul className="divide-y divide-gray-100">
              {events.map((event: any) => {
                const isProcessed = event.event_type === 'document_processed'
                const isDuplicate = event.event_type === 'duplicate_skipped'
                const isError = event.event_type === 'processing_error'
                const details = event.details || {}
                const time = new Date(event.created_at).toLocaleString('en-US', {
                  month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', hour12: true
                })

                return (
                  <li key={event.id} className="flex items-center gap-4 px-5 py-3">
                    {/* Icon */}
                    <div className={`flex h-9 w-9 flex-shrink-0 items-center justify-center rounded-full ${
                      isProcessed ? 'bg-green-100' : isDuplicate ? 'bg-amber-100' : 'bg-red-100'
                    }`}>
                      {isProcessed && (
                        <svg className="h-5 w-5 text-green-600" fill="none" viewBox="0 0 24 24" strokeWidth="2" stroke="currentColor">
                          <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                        </svg>
                      )}
                      {isDuplicate && (
                        <svg className="h-5 w-5 text-amber-600" fill="none" viewBox="0 0 24 24" strokeWidth="2" stroke="currentColor">
                          <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 17.25v3.375c0 .621-.504 1.125-1.125 1.125h-9.5a1.125 1.125 0 01-1.125-1.125V7.875c0-.621.504-1.125 1.125-1.125H6.25a9.06 9.06 0 011.5-.124m7.5 16.012H19.5a1.125 1.125 0 001.125-1.125V11.25a9 9 0 00-9-9h-1.125c-.621 0-1.125.504-1.125 1.125v7.5" />
                        </svg>
                      )}
                      {isError && (
                        <svg className="h-5 w-5 text-red-600" fill="none" viewBox="0 0 24 24" strokeWidth="2" stroke="currentColor">
                          <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                        </svg>
                      )}
                    </div>

                    {/* Content */}
                    <div className="min-w-0 flex-1">
                      <p className="text-sm font-medium text-gray-900 truncate">{event.file_name}</p>
                      <p className="text-xs text-gray-500">
                        {isProcessed && `Processed${details.total_pages ? ` — ${details.total_pages} page${details.total_pages > 1 ? 's' : ''}` : ''}`}
                        {isDuplicate && `Duplicate skipped — already ${details.existing_status || 'processed'}`}
                        {isError && `Error — ${(details.error_message || 'Unknown error').substring(0, 80)}`}
                      </p>
                    </div>

                    {/* Source + Time */}
                    <div className="flex-shrink-0 text-right">
                      <p className="text-xs text-gray-400">{time}</p>
                      <span className={`mt-1 inline-block rounded-full px-2 py-0.5 text-[10px] font-medium ${
                        event.source === 'folder_watcher' ? 'bg-purple-50 text-purple-600' : 'bg-gray-100 text-gray-500'
                      }`}>
                        {event.source === 'folder_watcher' ? 'Watcher' : 'Manual'}
                      </span>
                    </div>
                  </li>
                )
              })}
            </ul>
          </div>
        )}
      </div>
    </div>
  )
}
