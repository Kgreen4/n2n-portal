// Audit log utility — writes to security_audit_log table via Supabase.
// Used by frontend components to track user actions for HIPAA compliance.
//
// Usage:
//   import { logAuditEvent } from '@/lib/audit'
//   await logAuditEvent(supabase, {
//     action: 'document.export',
//     resourceType: 'eob_document',
//     resourceId: docId,
//     metadata: { batch_id: batchId, doc_count: 4 },
//   })

import type { SupabaseClient } from '@supabase/supabase-js'

interface AuditEvent {
  action: string
  resourceType?: string
  resourceId?: string
  metadata?: Record<string, unknown>
}

export async function logAuditEvent(
  supabase: SupabaseClient,
  event: AuditEvent,
) {
  try {
    // Get current user + practice from auth context
    const { data: { user } } = await supabase.auth.getUser()
    if (!user) return // No user = no audit (shouldn't happen in practice)

    const { data: link } = await supabase
      .from('practice_users')
      .select('practice_id')
      .eq('user_id', user.id)
      .limit(1)
      .single()

    await supabase.from('security_audit_log').insert({
      user_id: user.id,
      practice_id: link?.practice_id || null,
      action: event.action,
      resource_type: event.resourceType || null,
      resource_id: event.resourceId || null,
      metadata: event.metadata || {},
    })
  } catch (err) {
    // Non-fatal — audit logging should never break the user's workflow
    console.warn('[audit] Failed to log event:', err)
  }
}
