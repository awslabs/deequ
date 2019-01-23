CREATE TABLE public.example_table
(
    id integer,
    name varchar(255),
    description varchar(255),
    priority varchar(255),
    numviews integer
);
INSERT INTO public.example_table (id, name, description, priority, numviews) VALUES (1, 'Thingy A', 'awesome thing.', 'high', 0);
INSERT INTO public.example_table (id, name, description, priority, numviews) VALUES (2, 'Thingy B', 'available at http://thingb.com', null, 0);
INSERT INTO public.example_table (id, name, description, priority, numviews) VALUES (3, null, null, 'low', 5);
INSERT INTO public.example_table (id, name, description, priority, numviews) VALUES (4, 'Thingy D', 'checkout https://thingd.ca', 'low', 10);
INSERT INTO public.example_table (id, name, description, priority, numviews) VALUES (5, 'Thingy E', null, 'high', 12);