import { createServerClient } from '@supabase/ssr'
import { NextResponse, type NextRequest } from 'next/server'

export async function proxy(request: NextRequest) {
  // Create an unmodified response
  let supabaseResponse = NextResponse.next({
    request,
  })

  const supabase = createServerClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!,
    {
      cookies: {
        getAll() {
          return request.cookies.getAll()
        },
        setAll(cookiesToSet) {
          // Update the request cookies
          cookiesToSet.forEach(({ name, value }) => request.cookies.set(name, value))
          supabaseResponse = NextResponse.next({
            request,
          })
          // Update the response cookies
          cookiesToSet.forEach(({ name, value, options }) =>
            supabaseResponse.cookies.set(name, value, options)
          )
        },
      },
    }
  )

  // This will refresh the session if it's expired
  const { data: { user } } = await supabase.auth.getUser()

  // Route Protection Logic
  // If the user is NOT logged in and tries to access protected routes, redirect to /login
  if (
    !user &&
    (request.nextUrl.pathname.startsWith('/dashboard') ||
     request.nextUrl.pathname.startsWith('/upload') ||
     request.nextUrl.pathname.startsWith('/documents') ||
     request.nextUrl.pathname.startsWith('/inbox') ||
     request.nextUrl.pathname.startsWith('/settings'))
  ) {
    const url = request.nextUrl.clone()
    url.pathname = '/login'
    return NextResponse.redirect(url)
  }

  // If the user IS logged in and visits the root ('/') or '/login', send them to documents
  if (user && (request.nextUrl.pathname === '/' || request.nextUrl.pathname === '/login')) {
    const url = request.nextUrl.clone()
    url.pathname = '/documents'
    return NextResponse.redirect(url)
  }

  // Onboarding gate: if user is authenticated and accessing a protected route (not /setup),
  // check if they belong to a practice. If not, redirect to /setup.
  // Note: This DB query runs per protected page load. For MVP this is fine (<5ms on indexed PK).
  // Future optimization: put practice_id in user's JWT app_metadata to avoid DB hits.
  if (
    user &&
    !request.nextUrl.pathname.startsWith('/setup') &&
    (request.nextUrl.pathname.startsWith('/dashboard') ||
     request.nextUrl.pathname.startsWith('/upload') ||
     request.nextUrl.pathname.startsWith('/documents') ||
     request.nextUrl.pathname.startsWith('/inbox') ||
     request.nextUrl.pathname.startsWith('/settings'))
  ) {
    const { data: practiceLink } = await supabase
      .from('practice_users')
      .select('practice_id')
      .eq('user_id', user.id)
      .limit(1)

    if (!practiceLink || practiceLink.length === 0) {
      const url = request.nextUrl.clone()
      url.pathname = '/setup'
      return NextResponse.redirect(url)
    }
  }

  return supabaseResponse
}

// Specify which routes this proxy should run on
export const config = {
  matcher: [
    /*
     * Match all request paths except for the ones starting with:
     * - _next/static (static files)
     * - _next/image (image optimization files)
     * - favicon.ico (favicon file)
     */
    '/((?!_next/static|_next/image|favicon.ico|.*\\.(?:svg|png|jpg|jpeg|gif|webp)$).*)',
  ],
}
