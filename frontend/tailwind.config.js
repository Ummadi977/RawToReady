/** @type {import('tailwindcss').Config} */
export default {
  content: ['./src/**/*.{html,js,svelte,ts}'],
  theme: {
    extend: {
      colors: {
        bg: '#0a0a0f',
        'bg-2': '#0f0f18',
        surface: 'rgba(255,255,255,0.04)',
        'surface-2': 'rgba(255,255,255,0.07)',
        accent: '#6366f1',
        'accent-2': '#818cf8',
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'Fira Code', 'monospace'],
      },
      backdropBlur: {
        xs: '2px',
      },
    },
  },
  plugins: [],
};
