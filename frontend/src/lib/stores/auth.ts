/**
 * Auth store — JWT token, user, login/logout.
 * Uses Svelte 5 runes for reactivity.
 */

import { api } from '$lib/services/api.js';

interface User {
  id: string;
  username: string;
  email: string;
  role: string;
  is_active: boolean;
  created_at: string;
}

class AuthState {
  token = $state<string | null>(null);
  user = $state<User | null>(null);
  error = $state<string | null>(null);
  loading = $state(false);

  constructor() {
    if (typeof localStorage !== 'undefined') {
      this.token = localStorage.getItem('auth_token');
    }
  }

  get isAuthenticated(): boolean {
    return !!this.token;
  }

  async login(username: string, password: string): Promise<boolean> {
    this.loading = true;
    this.error = null;
    try {
      const response = await api.post<{ access_token: string; token_type: string; expires_in: number }>(
        '/api/auth/login',
        { username, password }
      );
      this.token = response.access_token;
      if (typeof localStorage !== 'undefined') {
        localStorage.setItem('auth_token', response.access_token);
      }
      // Fetch user info
      await this.fetchUser();
      return true;
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Login failed';
      this.error = msg;
      return false;
    } finally {
      this.loading = false;
    }
  }

  async fetchUser(): Promise<void> {
    if (!this.token) return;
    try {
      this.user = await api.get<User>('/api/auth/me');
    } catch {
      // Token might be invalid, clear it
      this.logout();
    }
  }

  logout(): void {
    this.token = null;
    this.user = null;
    if (typeof localStorage !== 'undefined') {
      localStorage.removeItem('auth_token');
    }
  }
}

export const auth = new AuthState();
