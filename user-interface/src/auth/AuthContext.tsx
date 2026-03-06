import React, { createContext, useContext, useEffect, useState, ReactNode } from 'react';
import {
  signIn as amplifySignIn,
  signOut as amplifySignOut,
  confirmSignIn,
  getCurrentUser,
  fetchAuthSession,
} from 'aws-amplify/auth';
import { AuthUser } from 'aws-amplify/auth';
import { AuthContextType, AuthError } from './types';

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export const AuthProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  const [user, setUser] = useState<AuthUser | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [requiresNewPassword, setRequiresNewPassword] = useState(false);

  const isAuthenticated = user !== null && !requiresNewPassword;

  useEffect(() => {
    (async () => {
      try {
        setUser(await getCurrentUser());
      } catch {
        setUser(null);
      } finally {
        setIsLoading(false);
      }
    })();
  }, []);

  const signIn = async (username: string, password: string) => {
    try {
      setIsLoading(true);
      const output = await amplifySignIn({ username, password });
      if (output.nextStep.signInStep === 'CONFIRM_SIGN_IN_WITH_NEW_PASSWORD_REQUIRED') {
        setRequiresNewPassword(true);
        return;
      }
      if (output.isSignedIn) {
        setUser(await getCurrentUser());
        setRequiresNewPassword(false);
      }
    } catch (error: any) {
      setUser(null);
      setRequiresNewPassword(false);
      const authError: AuthError = {
        type: error.name === 'NotAuthorizedException' ? 'AUTHENTICATION' : 'UNKNOWN',
        message: error.name === 'NotAuthorizedException' ? 'Invalid username or password' : (error.message || 'An unexpected error occurred'),
        details: error,
      };
      throw authError;
    } finally {
      setIsLoading(false);
    }
  };

  const completeNewPassword = async (newPassword: string) => {
    try {
      setIsLoading(true);
      const output = await confirmSignIn({ challengeResponse: newPassword });
      if (output.isSignedIn) {
        setUser(await getCurrentUser());
        setRequiresNewPassword(false);
      }
    } catch (error: any) {
      throw { type: 'UNKNOWN', message: error.message || 'Failed to set new password', details: error } as AuthError;
    } finally {
      setIsLoading(false);
    }
  };

  const signOut = async () => {
    try {
      setIsLoading(true);
      await amplifySignOut();
    } finally {
      setUser(null);
      setIsLoading(false);
    }
  };

  const getAccessToken = async () => {
    const session = await fetchAuthSession();
    if (!session.tokens?.accessToken) throw new Error('No access token');
    return session.tokens.accessToken.toString();
  };

  return (
    <AuthContext.Provider value={{ user, isAuthenticated, isLoading, requiresNewPassword, signIn, signOut, completeNewPassword, getAccessToken }}>
      {children}
    </AuthContext.Provider>
  );
};

export const useAuth = (): AuthContextType => {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used within AuthProvider');
  return ctx;
};
