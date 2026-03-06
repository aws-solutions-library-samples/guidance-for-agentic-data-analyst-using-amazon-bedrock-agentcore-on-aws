import React, { useState } from 'react';
import { Container, Header, Form, FormField, Input, Button, SpaceBetween, Alert, Box } from '@cloudscape-design/components';
import { AuthError } from './types';

interface LoginFormProps {
  onSignIn: (username: string, password: string) => Promise<void>;
  isLoading: boolean;
  error?: AuthError | null;
}

export const LoginForm: React.FC<LoginFormProps> = ({ onSignIn, isLoading, error }) => {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (username.trim() && password) await onSignIn(username.trim(), password);
  };

  return (
    <Box margin="xxl">
      <Container header={<Header variant="h1">Data Analyst Agent</Header>}>
        <form onSubmit={handleSubmit}>
          <Form
            actions={<Button variant="primary" formAction="submit" loading={isLoading} disabled={isLoading}>Sign In</Button>}
            errorText={error?.message}
          >
            <SpaceBetween size="l">
              {error && <Alert type="error" header="Authentication Error">{error.message}</Alert>}
              <FormField label="Username">
                <Input value={username} onChange={({ detail }) => setUsername(detail.value)} disabled={isLoading} autoComplete="username" />
              </FormField>
              <FormField label="Password">
                <Input value={password} onChange={({ detail }) => setPassword(detail.value)} type="password" disabled={isLoading} autoComplete="current-password" />
              </FormField>
            </SpaceBetween>
          </Form>
        </form>
      </Container>
    </Box>
  );
};
