// Simple full-screen layout for the onboarding wizard.
// No sidebar or dashboard nav — keeps new users focused on setup.
export default function OnboardingLayout({ children }: { children: React.ReactNode }) {
  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 via-blue-50 to-indigo-100">
      <div className="flex min-h-screen items-center justify-center p-4">
        {children}
      </div>
    </div>
  )
}
