-- Run these while the pipeline is up, then ask the agent again.
UPDATE public.orders SET status = 'shipped' WHERE id = 104;
INSERT INTO public.orders VALUES (105, 2, 'pending', 45000, now());
INSERT INTO public.order_items VALUES (10, 105, 'FAN-KIT', 9, 5000);
DELETE FROM public.order_items WHERE id = 2;   -- order 100 loses its PDUs
DELETE FROM public.orders WHERE id = 101;      -- order 101 disappears everywhere
