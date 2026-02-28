import { redirect } from 'next/navigation'
import { createClient } from '@/lib/supabase/server'
import DocumentsClient from './DocumentsClient'

export default async function DocumentsPage() {
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

  const { data: documents } = await supabase
    .from('eob_documents')
    .select('id, file_name, status, total_pages, items_extracted, created_at, practice_id, last_exported_at, export_batch_id, export_total_paid, export_total_patient_resp, export_claim_count, export_found_revenue_amount, export_found_revenue_count, has_found_revenue')
    .eq('practice_id', practiceLink.practice_id)
    .order('created_at', { ascending: false })

  return (
    <DocumentsClient
      documents={documents || []}
      practiceId={practiceLink.practice_id}
    />
  )
}
