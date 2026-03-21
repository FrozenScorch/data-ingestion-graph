// Disable SSR for the entire app - this is a client-side SPA
// that communicates with the FastAPI backend API.
// Svelte 5 $state runes in store classes are not available during SSR.
export const ssr = false;
