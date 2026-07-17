import { useCallback, useState } from "react";
import type { OnboardingConfig, OnboardingState } from "./types";

const STORAGE_KEY = "keel.onboarding.v1";

const defaultState: OnboardingState = {
  completed: false,
  config: null,
  completedAt: null,
};

function readState(): OnboardingState {
  if (typeof window === "undefined") return defaultState;
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return defaultState;
    const parsed = JSON.parse(raw) as Partial<OnboardingState>;
    return {
      completed: Boolean(parsed.completed),
      config: parsed.config ?? null,
      completedAt: parsed.completedAt ?? null,
    };
  } catch {
    return defaultState;
  }
}

function writeState(state: OnboardingState): void {
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  } catch {
    // Storage may be unavailable (privacy mode); the wizard still works for this session.
  }
}

/**
 * First-run onboarding is entirely local: it never calls a backend and is
 * truthful about that in its own copy. `complete` records that the wizard ran
 * and stores the (local-only) workspace name choice; `reset` lets a user
 * replay the tour from Settings.
 */
export function useOnboarding() {
  const [state, setState] = useState<OnboardingState>(() => readState());

  const complete = useCallback((config: OnboardingConfig) => {
    const next: OnboardingState = {
      completed: true,
      config,
      completedAt: new Date().toISOString(),
    };
    writeState(next);
    setState(next);
  }, []);

  const reset = useCallback(() => {
    writeState(defaultState);
    setState(defaultState);
  }, []);

  return { ...state, complete, reset };
}

export function isOnboardingComplete(): boolean {
  return readState().completed;
}
