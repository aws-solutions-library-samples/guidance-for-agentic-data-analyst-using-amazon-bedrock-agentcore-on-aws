import { AppLayout } from '@cloudscape-design/components';
import '@cloudscape-design/global-styles/index.css';
import { ChatPane } from './components/ChatPane';
import { AuthProvider, LoginForm, NewPasswordForm, useAuth } from './auth';
import { useState } from 'react';
import { AuthError } from './auth/types';

function LoginPage() {
  const { signIn, isLoading, requiresNewPassword, completeNewPassword } = useAuth();
  const [error, setError] = useState<AuthError | null>(null);

  const handleSignIn = async (username: string, password: string) => {
    try { setError(null); await signIn(username, password); }
    catch (err) { setError(err as AuthError); }
  };

  const handleNewPassword = async (newPassword: string) => {
    try { setError(null); await completeNewPassword(newPassword); }
    catch (err) { setError(err as AuthError); }
  };

  if (requiresNewPassword) return <NewPasswordForm onCompleteNewPassword={handleNewPassword} isLoading={isLoading} error={error} />;
  return <LoginForm onSignIn={handleSignIn} isLoading={isLoading} error={error} />;
}

function AppContent() {
  const { isAuthenticated } = useAuth();
  return isAuthenticated ? (
    <AppLayout content={<ChatPane />} navigationHide toolsHide disableContentPaddings />
  ) : (
    <LoginPage />
  );
}

function App() {
  return (
    <AuthProvider>
      <AppContent />
    </AuthProvider>
  );
}

export default App;
