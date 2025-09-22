import React from "react";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Form, FormControl, FormField, FormItem, FormLabel, FormMessage } from "@/components/ui/form";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";

const editServerSchema = z.object({
  name: z.string().min(1, "Server name is required"),
  host: z.string().min(1, "Host is required"),
  port: z.coerce.number().min(1).max(65535, "Port must be between 1-65535"),
  username: z.string().min(1, "Username is required"),
  password: z.string().optional()
});

type EditServerFormData = z.infer<typeof editServerSchema>;

export interface ServerData {
  id: string;
  name: string;
  host: string;
  port: number;
  username: string;
}

export interface EditServerDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  server: ServerData | null;
  onUpdateServer: (id: string, updates: EditServerFormData) => void;
  onDeleteServer: (id: string) => void;
  isUpdating?: boolean;
  isDeleting?: boolean;
}

export default function EditServerDialog({ 
  open, 
  onOpenChange, 
  server,
  onUpdateServer,
  onDeleteServer,
  isUpdating = false,
  isDeleting = false
}: EditServerDialogProps) {
  const form = useForm<EditServerFormData>({
    resolver: zodResolver(editServerSchema),
    defaultValues: {
      name: '',
      host: '',
      port: 22,
      username: '',
      password: ''
    }
  });

  // Reset form when server changes
  React.useEffect(() => {
    if (server) {
      form.reset({
        name: server.name,
        host: server.host,
        port: server.port,
        username: server.username,
        password: '' // Always start with empty password for security
      });
    }
  }, [server, form]);

  const onSubmit = (data: EditServerFormData) => {
    if (!server) return;
    
    // Only include password if it's provided
    const updates = { ...data };
    if (!updates.password) {
      delete updates.password;
    }
    
    onUpdateServer(server.id, updates);
  };

  const handleDelete = () => {
    if (!server) return;
    onDeleteServer(server.id);
  };

  if (!server) return null;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md" data-testid="dialog-edit-server">
        <DialogHeader>
          <DialogTitle>Edit Server</DialogTitle>
        </DialogHeader>
        <Form {...form}>
          <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-4">
            <FormField
              control={form.control}
              name="name"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Server Name</FormLabel>
                  <FormControl>
                    <Input 
                      placeholder="My Production Server" 
                      {...field} 
                      data-testid="input-edit-server-name"
                    />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            
            <FormField
              control={form.control}
              name="host"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Host</FormLabel>
                  <FormControl>
                    <Input 
                      placeholder="192.168.1.100 or server.example.com" 
                      {...field} 
                      data-testid="input-edit-host"
                    />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            
            <div className="grid grid-cols-2 gap-4">
              <FormField
                control={form.control}
                name="port"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Port</FormLabel>
                    <FormControl>
                      <Input 
                        type="number" 
                        placeholder="22" 
                        {...field} 
                        data-testid="input-edit-port"
                      />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
              
              <FormField
                control={form.control}
                name="username"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Username</FormLabel>
                    <FormControl>
                      <Input 
                        placeholder="admin" 
                        {...field} 
                        data-testid="input-edit-username"
                      />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
            </div>
            
            <FormField
              control={form.control}
              name="password"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Password (optional)</FormLabel>
                  <FormControl>
                    <Input 
                      type="password" 
                      placeholder="Leave empty to keep current password" 
                      {...field} 
                      data-testid="input-edit-password"
                    />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            
            <div className="flex gap-2 pt-4">
              <Button
                type="submit"
                disabled={isUpdating}
                className="flex-1"
                data-testid="button-update-server"
              >
                {isUpdating ? 'Updating...' : 'Update Server'}
              </Button>
              
              <Button
                type="button"
                variant="destructive"
                onClick={handleDelete}
                disabled={isDeleting}
                data-testid="button-delete-server"
              >
                {isDeleting ? 'Deleting...' : 'Delete'}
              </Button>
            </div>
          </form>
        </Form>
      </DialogContent>
    </Dialog>
  );
}