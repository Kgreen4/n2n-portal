-- Phase 15b: Add trial_ends_at to practices table
-- Tracks when the 7-day free trial expires for each practice.
-- Set by create-practice edge function to now() + 7 days on signup.

ALTER TABLE public.practices
  ADD COLUMN IF NOT EXISTS trial_ends_at timestamptz;
