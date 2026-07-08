/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        bg: "var(--bg)",
        surface: "var(--surface)",
        "surface-2": "var(--surface-2)",
        border: "var(--border)",
        text: "var(--text)",
        "text-soft": "var(--text-soft)",
        "text-muted": "var(--text-muted)",
        accent: "var(--accent)",
        green: "var(--green)",
        amber: "var(--amber)",
        red: "var(--red)",
        sky: "var(--sky)",
      },
      borderRadius: { DEFAULT: "10px", sm: "7px" },
      boxShadow: {
        card: "0 1px 2px rgba(16,21,31,.04), 0 1px 3px rgba(16,21,31,.06)",
      },
    },
  },
  plugins: [],
}

