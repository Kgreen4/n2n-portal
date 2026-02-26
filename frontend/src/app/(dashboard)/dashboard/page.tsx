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

  // Fetch practice details and document count in parallel
  const [practiceResult, docCountResult] = await Promise.all([
    supabase
      .from('practice_credits')
      .select('credits_remaining')
      .eq('practice_id', practiceId)
      .single(),
    supabase
      .from('eob_documents')
      .select('*', { count: 'exact', head: true })
      .eq('practice_id', practiceId),
  ])

  const credits = practiceResult.data?.credits_remaining ?? 0
  const docCount = docCountResult.count

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
    </div>
  )
}
