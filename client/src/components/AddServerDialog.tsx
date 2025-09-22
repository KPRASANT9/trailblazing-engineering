import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Form, FormControl, FormField, FormItem, FormLabel, FormMessage } from "@/components/ui/form";
import { Plus } from "lucide-react";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import { useState } from "react";

const serverSchema = z.object({
  name: z.string().min(1, "Server name is required"),
  host: z.string().min(1, "Host is required"),
  port: z.coerce.number().min(1).max(65535, "Port must be between 1-65535"),
  username: z.string().min(1, "Username is required"),
  password: z.string().min(1, "Password is required")
});

type ServerFormData = z.infer<typeof serverSchema>;

export interface AddServerDialogProps {
  onAddServer: (server: ServerFormData) => void;
}

export default function AddServerDialog({ onAddServer }: AddServerDialogProps) {
  const [open, setOpen] = useState(false);
  
  const form = useForm<ServerFormData>({
    resolver: zodResolver(serverSchema),
    defaultValues: {
      name: '',
      host: '',
      port: 22,
      username: '',
      password: ''
    }
  });

  const onSubmit = (data: ServerFormData) => {
    console.log('Adding server:', data);
    onAddServer(data);
    form.reset();
    setOpen(false);
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button data-testid="button-add-server">
          <Plus className="h-4 w-4 mr-2" />
          Add Server
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-md" data-testid="dialog-add-server">
        <DialogHeader>
          <DialogTitle>Add New Server</DialogTitle>
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
                      data-testid="input-server-name"
                    />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <div className="grid grid-cols-3 gap-4">
              <FormField
                control={form.control}
                name="host"
                render={({ field }) => (
                  <FormItem className="col-span-2">
                    <FormLabel>Host</FormLabel>
                    <FormControl>
                      <Input 
                        placeholder="192.168.1.100" 
                        {...field} 
                        data-testid="input-host"
                      />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
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
                        data-testid="input-port"
                      />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
            </div>
            <FormField
              control={form.control}
              name="username"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Username</FormLabel>
                  <FormControl>
                    <Input 
                      placeholder="root" 
                      {...field} 
                      data-testid="input-username"
                    />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <FormField
              control={form.control}
              name="password"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Password</FormLabel>
                  <FormControl>
                    <Input 
                      type="password" 
                      placeholder="Password" 
                      {...field} 
                      data-testid="input-password"
                    />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <div className="flex justify-end gap-2">
              <Button 
                type="button" 
                variant="outline" 
                onClick={() => setOpen(false)}
                data-testid="button-cancel"
              >
                Cancel
              </Button>
              <Button type="submit" data-testid="button-save-server">
                Add Server
              </Button>
            </div>
          </form>
        </Form>
      </DialogContent>
    </Dialog>
  );
}