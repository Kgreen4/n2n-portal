# deploy-stripe.ps1 — Run this from Windows PowerShell (not Git Bash)
# Deploys all Phase 15a Stripe billing changes
#
# Usage: Right-click > Run with PowerShell  (or open PowerShell and run: .\deploy-stripe.ps1)

$ProjectRoot = "C:\Users\kgree\Dropbox\N2N\Business Offerings\EOB extraction\Claude Code"
$FrontendDir = "$ProjectRoot\frontend"
$SupabaseRef = "jdmyjdvricpyrsfchakk"

Set-Location $ProjectRoot

Write-Host "`n=== Step 1: Supabase Login ===" -ForegroundColor Cyan
Write-Host "A browser window will open. Log in to Supabase and paste the token."
npx supabase login

Write-Host "`n=== Step 2: Set Supabase Secrets ===" -ForegroundColor Cyan
npx supabase secrets set `
  STRIPE_SECRET_KEY="REDACTED_STRIPE_SECRET_KEY" `
  STRIPE_WEBHOOK_SECRET="REDACTED_STRIPE_WEBHOOK_SECRET" `
  STRIPE_STARTER_PRICE_ID="price_1T9YSv0JXufBkchGE2BB40w3" `
  STRIPE_PRO_PRICE_ID="price_1T9YTk0JXufBkchGaQlXJG58" `
  STRIPE_BOOST100_PRICE_ID="price_1T9YUI0JXufBkchGOGwuJIhc" `
  STRIPE_BOOST500_PRICE_ID="price_1T9YUh0JXufBkchGOfe66wc1" `
  --project-ref $SupabaseRef

Write-Host "`n=== Step 3: Deploy Edge Functions ===" -ForegroundColor Cyan
npx supabase functions deploy create-checkout-session --project-ref $SupabaseRef
npx supabase functions deploy create-portal-session --project-ref $SupabaseRef
npx supabase functions deploy stripe-webhook --project-ref $SupabaseRef --no-verify-jwt
npx supabase functions deploy eob-enqueue --project-ref $SupabaseRef
npx supabase functions deploy generate-835 --project-ref $SupabaseRef

Write-Host "`n=== Step 4: Vercel Login ===" -ForegroundColor Cyan
Set-Location $FrontendDir
Write-Host "A browser window will open. Log in to Vercel."
npx vercel login

Write-Host "`n=== Step 5: Set Vercel Environment Variables ===" -ForegroundColor Cyan
Write-Host "Adding NEXT_PUBLIC_STRIPE_STARTER_PRICE_ID..."
"price_1T9YSv0JXufBkchGE2BB40w3" | npx vercel env add NEXT_PUBLIC_STRIPE_STARTER_PRICE_ID production --force
"price_1T9YSv0JXufBkchGE2BB40w3" | npx vercel env add NEXT_PUBLIC_STRIPE_STARTER_PRICE_ID preview --force
"price_1T9YSv0JXufBkchGE2BB40w3" | npx vercel env add NEXT_PUBLIC_STRIPE_STARTER_PRICE_ID development --force

Write-Host "Adding NEXT_PUBLIC_STRIPE_PRO_PRICE_ID..."
"price_1T9YTk0JXufBkchGaQlXJG58" | npx vercel env add NEXT_PUBLIC_STRIPE_PRO_PRICE_ID production --force
"price_1T9YTk0JXufBkchGaQlXJG58" | npx vercel env add NEXT_PUBLIC_STRIPE_PRO_PRICE_ID preview --force
"price_1T9YTk0JXufBkchGaQlXJG58" | npx vercel env add NEXT_PUBLIC_STRIPE_PRO_PRICE_ID development --force

Write-Host "Adding NEXT_PUBLIC_STRIPE_BOOST100_PRICE_ID..."
"price_1T9YUI0JXufBkchGOGwuJIhc" | npx vercel env add NEXT_PUBLIC_STRIPE_BOOST100_PRICE_ID production --force
"price_1T9YUI0JXufBkchGOGwuJIhc" | npx vercel env add NEXT_PUBLIC_STRIPE_BOOST100_PRICE_ID preview --force
"price_1T9YUI0JXufBkchGOGwuJIhc" | npx vercel env add NEXT_PUBLIC_STRIPE_BOOST100_PRICE_ID development --force

Write-Host "Adding NEXT_PUBLIC_STRIPE_BOOST500_PRICE_ID..."
"price_1T9YUh0JXufBkchGOfe66wc1" | npx vercel env add NEXT_PUBLIC_STRIPE_BOOST500_PRICE_ID production --force
"price_1T9YUh0JXufBkchGOfe66wc1" | npx vercel env add NEXT_PUBLIC_STRIPE_BOOST500_PRICE_ID preview --force
"price_1T9YUh0JXufBkchGOfe66wc1" | npx vercel env add NEXT_PUBLIC_STRIPE_BOOST500_PRICE_ID development --force

Write-Host "`n=== Step 6: Deploy Frontend to Vercel ===" -ForegroundColor Cyan
npx vercel --prod

Write-Host "`n=== All done! ===" -ForegroundColor Green
Write-Host "Test the billing page: https://your-vercel-url/billing"
Write-Host "Test card: 4242 4242 4242 4242, any future date, any CVC"
