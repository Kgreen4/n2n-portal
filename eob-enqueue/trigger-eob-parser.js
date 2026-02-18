import { serve } from "https://deno.land/std@0.168.0/http/server.ts"
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'

serve(async (req) => {
  try {
    const { practice_id, gcs_object_name } = await req.json()
    
    const supabase = createClient(
      Deno.env.get('SUPABASE_URL')!,
      Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!
    )
    
    // 1. Check and use credit
    const { data: creditUsed, error: creditError } = await supabase.rpc(
      'use_parsing_credit',
      { practice_id }
    )
    
    if (creditError) {
      console.error('Credit check error:', creditError)
      return new Response(
        JSON.stringify({ error: 'Failed to verify credits', details: creditError.message }),
        { status: 500, headers: { 'Content-Type': 'application/json' } }
      )
    }
    
    if (!creditUsed) {
      return new Response(
        JSON.stringify({ 
          error: 'Insufficient credits',
          credits_remaining: 0
        }),
        { status: 402, headers: { 'Content-Type': 'application/json' } }
      )
    }
    
    // 2. Create eob_documents entry (let database generate UUID)
    const { data: docData, error: docError } = await supabase
      .from('eob_documents')
      .insert({
        practice_id,
        file_path: gcs_object_name,
        file_name: gcs_object_name.split('/').pop(),
        status: 'processing'
      })
      .select()
      .single()
    
    if (docError) {
      console.error('Document creation error:', docError)
      await supabase.rpc('refund_parsing_credit', { practice_id })
      return new Response(
        JSON.stringify({ error: 'Failed to create document', details: docError.message }),
        { status: 500, headers: { 'Content-Type': 'application/json' } }
      )
    }
    
    const eob_document_id = docData.id
    
    // 3. Create processing log
    const { error: logError } = await supabase
      .from('eob_processing_logs')
      .insert({
        practice_id,
        eob_document_id,
        gcs_object_name,
        status: 'pending',
        credits_used: 1
      })
    
    if (logError) {
      console.error('Log creation error:', logError)
      await supabase.rpc('refund_parsing_credit', { practice_id })
      await supabase
        .from('eob_documents')
        .update({ status: 'failed', error_message: logError.message })
        .eq('id', eob_document_id)
      return new Response(
        JSON.stringify({ error: 'Failed to create processing log', details: logError.message }),
        { status: 500, headers: { 'Content-Type': 'application/json' } }
      )
    }
    
    // 4. Trigger n8n workflow
    const n8nPayload = {
      practice_id,
      eob_document_id,
      gcs_bucket: 'cardio-metrics-eob-uploads',
      gcs_object_name,
      gcp_project: 'cardio-metrics-dev',
      bigquery_dataset: 'billing_audit_practice_test',
      callback_url: `${Deno.env.get('APP_URL')}/api/v1/webhook/parsing-complete`,
      webhook_secret: Deno.env.get('N8N_WEBHOOK_SECRET')
    }
    
    const n8nResponse = await fetch(
      `${Deno.env.get('N8N_WEBHOOK_URL')}/webhook/eob-parse`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${Deno.env.get('N8N_WEBHOOK_SECRET')}`
        },
        body: JSON.stringify(n8nPayload)
      }
    )
    
    if (!n8nResponse.ok) {
      await supabase.rpc('refund_parsing_credit', { practice_id })
      await supabase
        .from('eob_documents')
        .update({ 
          status: 'failed',
          error_message: `n8n trigger failed: ${n8nResponse.statusText}`
        })
        .eq('id', eob_document_id)
      await supabase
        .from('eob_processing_logs')
        .update({ 
          status: 'failed',
          error_message: `n8n trigger failed: ${n8nResponse.statusText}`,
          processing_completed_at: new Date().toISOString()
        })
        .eq('eob_document_id', eob_document_id)
      
      throw new Error(`n8n trigger failed: ${n8nResponse.statusText}`)
    }
    
    // 5. Update to 'processing'
    await supabase
      .from('eob_processing_logs')
      .update({ status: 'processing' })
      .eq('eob_document_id', eob_document_id)
    
    // 6. Get remaining credits
    const { data: profile } = await supabase
      .from('practice_credits')
      .select('credit_balance')
      .eq('id', practice_id)
      .single()
    
    return new Response(
      JSON.stringify({
        success: true,
        eob_document_id,
        credits_remaining: profile?.credit_balance || 0,
        status: 'processing'
      }),
      { status: 200, headers: { 'Content-Type': 'application/json' } }
    )
    
  } catch (error) {
    console.error('Edge function error:', error)
    return new Response(
      JSON.stringify({ error: error.message }),
      { status: 500, headers: { 'Content-Type': 'application/json' } }
    )
  }
})