import type { ButtonHTMLAttributes } from "react";
import { cn } from "../../lib/cn";

const variants = {
  primary: "bg-accent text-white border-accent hover:opacity-90",
  danger: "bg-red text-white border-red hover:opacity-90",
  default: "bg-surface text-text border-border hover:bg-surface-2",
};

export function Button({
  variant = "default",
  className,
  ...props
}: ButtonHTMLAttributes<HTMLButtonElement> & { variant?: keyof typeof variants }) {
  return (
    <button
      className={cn(
        "inline-flex items-center gap-1.5 rounded-sm border px-3.5 py-2 text-sm font-semibold",
        "disabled:cursor-default disabled:opacity-50",
        variants[variant],
        className,
      )}
      {...props}
    />
  );
}
