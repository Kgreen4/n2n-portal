import { createClient } from '@/lib/supabase/server'
import { redirect } from 'next/navigation'
import PdfUploader from '@/components/PdfUploader'

export default async function UploadPage() {
  const supabase = await createClient()
  const { data: { user } } = await supabase.auth.getUser()

  if (!user) redirect('/login')

  // Get the practice ID for the current user
  const { data: practiceLink } = await supabase
    .from('practice_users')
    .select('practice_id')
    .eq('user_id', user.id)
    .single()

  if (!practiceLink?.practice_id) {
    redirect('/setup')
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Process New EOB</h1>
        <p className="mt-1 text-sm text-gray-500">
          Upload a scanned EOB or remittance PDF to extract line items automatically.
        </p>
      </div>

      <PdfUploader practiceId={practiceLink.practice_id} />
    </div>
  )
}
