import React, { useState } from 'react';
import { Container, Header, Form, FormField, Input, Button, SpaceBetween, Alert, Box } from '@cloudscape-design/components';
import { AuthError } from './types';

interface NewPasswordFormProps {
  onCompleteNewPassword: (newPassword: string) => Promise<void>;
  isLoading: boolean;
  error?: AuthError | null;
}

export const NewPasswordForm: React.FC<NewPasswordFormProps> = ({ onCompleteNewPassword, isLoading, error }) => {
  const [newPassword, setNewPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [mismatch, setMismatch] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (newPassword !== confirmPassword) { setMismatch(true); return; }
    setMismatch(false);
    await onCompleteNewPassword(newPassword);
  };

  return (
    <Box margin="xxl">
      <Container header={<Header variant="h1">Set New Password</Header>}>
        <form onSubmit={handleSubmit}>
          <Form actions={<Button variant="primary" formAction="submit" loading={isLoading} disabled={isLoading}>Set Password</Button>}>
            <SpaceBetween size="l">
              <Alert type="info" header="Password Change Required">You must change your temporary password before continuing.</Alert>
              {error && <Alert type="error">{error.message}</Alert>}
              {mismatch && <Alert type="error">Passwords do not match</Alert>}
              <FormField label="New Password" description="Min 8 chars, uppercase, lowercase, number, special character">
                <Input value={newPassword} onChange={({ detail }) => setNewPassword(detail.value)} type="password" disabled={isLoading} />
              </FormField>
              <FormField label="Confirm Password">
                <Input value={confirmPassword} onChange={({ detail }) => setConfirmPassword(detail.value)} type="password" disabled={isLoading} />
              </FormField>
            </SpaceBetween>
          </Form>
        </form>
      </Container>
    </Box>
  );
};
