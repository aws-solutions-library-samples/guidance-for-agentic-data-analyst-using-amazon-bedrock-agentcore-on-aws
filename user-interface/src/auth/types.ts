import { AuthUser } from 'aws-amplify/auth';

export interface AuthContextType {
  user: AuthUser | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  requiresNewPassword: boolean;
  signIn: (username: string, password: string) => Promise<void>;
  signOut: () => Promise<void>;
  completeNewPassword: (newPassword: string) => Promise<void>;
  getAccessToken: () => Promise<string>;
}

export interface AuthError {
  type: 'AUTHENTICATION' | 'NETWORK' | 'VALIDATION' | 'UNKNOWN';
  message: string;
  details?: any;
}
