export interface OnboardingConfig {
  workspaceName: string;
}

export interface OnboardingState {
  completed: boolean;
  config: OnboardingConfig | null;
  completedAt: string | null;
}
