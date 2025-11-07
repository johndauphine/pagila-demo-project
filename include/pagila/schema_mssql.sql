-- Pagila schema for SQL Server
-- Simplified version focusing on core tables without complex Postgres features

-- Switch to pagila_target database
USE pagila_target;

-- Language table
CREATE TABLE [dbo].[language] (
    [language_id] INT IDENTITY(1,1) PRIMARY KEY,
    [name] NVARCHAR(20) NOT NULL,
    [last_update] DATETIME2 NOT NULL DEFAULT GETDATE()
);

-- Category table
CREATE TABLE [dbo].[category] (
    [category_id] INT IDENTITY(1,1) PRIMARY KEY,
    [name] NVARCHAR(25) NOT NULL,
    [last_update] DATETIME2 NOT NULL DEFAULT GETDATE()
);

-- Actor table
CREATE TABLE [dbo].[actor] (
    [actor_id] INT IDENTITY(1,1) PRIMARY KEY,
    [first_name] NVARCHAR(45) NOT NULL,
    [last_name] NVARCHAR(45) NOT NULL,
    [last_update] DATETIME2 NOT NULL DEFAULT GETDATE()
);

CREATE INDEX [idx_actor_last_name] ON [dbo].[actor]([last_name]);

-- Film table
CREATE TABLE [dbo].[film] (
    [film_id] INT IDENTITY(1,1) PRIMARY KEY,
    [title] NVARCHAR(255) NOT NULL,
    [description] NVARCHAR(MAX),
    [release_year] INT,
    [language_id] INT NOT NULL,
    [original_language_id] INT,
    [rental_duration] SMALLINT NOT NULL DEFAULT 3,
    [rental_rate] DECIMAL(4,2) NOT NULL DEFAULT 4.99,
    [length] SMALLINT,
    [replacement_cost] DECIMAL(5,2) NOT NULL DEFAULT 19.99,
    [rating] NVARCHAR(10) DEFAULT 'G',
    [special_features] NVARCHAR(MAX),
    [last_update] DATETIME2 NOT NULL DEFAULT GETDATE(),
    CONSTRAINT [fk_film_language] FOREIGN KEY ([language_id]) 
        REFERENCES [dbo].[language]([language_id]),
    CONSTRAINT [fk_film_original_language] FOREIGN KEY ([original_language_id]) 
        REFERENCES [dbo].[language]([language_id])
);

CREATE INDEX [idx_film_language_id] ON [dbo].[film]([language_id]);
CREATE INDEX [idx_film_title] ON [dbo].[film]([title]);

-- Film-Actor junction table
CREATE TABLE [dbo].[film_actor] (
    [actor_id] INT NOT NULL,
    [film_id] INT NOT NULL,
    [last_update] DATETIME2 NOT NULL DEFAULT GETDATE(),
    CONSTRAINT [pk_film_actor] PRIMARY KEY ([actor_id], [film_id]),
    CONSTRAINT [fk_film_actor_actor] FOREIGN KEY ([actor_id]) 
        REFERENCES [dbo].[actor]([actor_id]),
    CONSTRAINT [fk_film_actor_film] FOREIGN KEY ([film_id]) 
        REFERENCES [dbo].[film]([film_id])
);

CREATE INDEX [idx_film_actor_film_id] ON [dbo].[film_actor]([film_id]);

-- Film-Category junction table
CREATE TABLE [dbo].[film_category] (
    [film_id] INT NOT NULL,
    [category_id] INT NOT NULL,
    [last_update] DATETIME2 NOT NULL DEFAULT GETDATE(),
    CONSTRAINT [pk_film_category] PRIMARY KEY ([film_id], [category_id]),
    CONSTRAINT [fk_film_category_film] FOREIGN KEY ([film_id]) 
        REFERENCES [dbo].[film]([film_id]),
    CONSTRAINT [fk_film_category_category] FOREIGN KEY ([category_id]) 
        REFERENCES [dbo].[category]([category_id])
);

-- Country table
CREATE TABLE [dbo].[country] (
    [country_id] INT IDENTITY(1,1) PRIMARY KEY,
    [country] NVARCHAR(50) NOT NULL,
    [last_update] DATETIME2 NOT NULL DEFAULT GETDATE()
);

-- City table
CREATE TABLE [dbo].[city] (
    [city_id] INT IDENTITY(1,1) PRIMARY KEY,
    [city] NVARCHAR(50) NOT NULL,
    [country_id] INT NOT NULL,
    [last_update] DATETIME2 NOT NULL DEFAULT GETDATE(),
    CONSTRAINT [fk_city_country] FOREIGN KEY ([country_id]) 
        REFERENCES [dbo].[country]([country_id])
);

CREATE INDEX [idx_city_country_id] ON [dbo].[city]([country_id]);

-- Address table
CREATE TABLE [dbo].[address] (
    [address_id] INT IDENTITY(1,1) PRIMARY KEY,
    [address] NVARCHAR(50) NOT NULL,
    [address2] NVARCHAR(50),
    [district] NVARCHAR(20) NOT NULL,
    [city_id] INT NOT NULL,
    [postal_code] NVARCHAR(10),
    [phone] NVARCHAR(20) NOT NULL,
    [last_update] DATETIME2 NOT NULL DEFAULT GETDATE(),
    CONSTRAINT [fk_address_city] FOREIGN KEY ([city_id]) 
        REFERENCES [dbo].[city]([city_id])
);

CREATE INDEX [idx_address_city_id] ON [dbo].[address]([city_id]);

-- Store table
CREATE TABLE [dbo].[store] (
    [store_id] INT IDENTITY(1,1) PRIMARY KEY,
    [manager_staff_id] INT NOT NULL,
    [address_id] INT NOT NULL,
    [last_update] DATETIME2 NOT NULL DEFAULT GETDATE()
);

-- Customer table
CREATE TABLE [dbo].[customer] (
    [customer_id] INT IDENTITY(1,1) PRIMARY KEY,
    [store_id] INT NOT NULL,
    [first_name] NVARCHAR(45) NOT NULL,
    [last_name] NVARCHAR(45) NOT NULL,
    [email] NVARCHAR(50),
    [address_id] INT NOT NULL,
    [active] BIT NOT NULL DEFAULT 1,
    [create_date] DATE NOT NULL,
    [last_update] DATETIME2 NOT NULL DEFAULT GETDATE(),
    CONSTRAINT [fk_customer_address] FOREIGN KEY ([address_id]) 
        REFERENCES [dbo].[address]([address_id]),
    CONSTRAINT [fk_customer_store] FOREIGN KEY ([store_id]) 
        REFERENCES [dbo].[store]([store_id])
);

CREATE INDEX [idx_customer_last_name] ON [dbo].[customer]([last_name]);
CREATE INDEX [idx_customer_address_id] ON [dbo].[customer]([address_id]);
CREATE INDEX [idx_customer_store_id] ON [dbo].[customer]([store_id]);

-- Staff table
CREATE TABLE [dbo].[staff] (
    [staff_id] INT IDENTITY(1,1) PRIMARY KEY,
    [first_name] NVARCHAR(45) NOT NULL,
    [last_name] NVARCHAR(45) NOT NULL,
    [address_id] INT NOT NULL,
    [email] NVARCHAR(50),
    [store_id] INT NOT NULL,
    [active] BIT NOT NULL DEFAULT 1,
    [username] NVARCHAR(50) NOT NULL,
    [password] NVARCHAR(40),
    [last_update] DATETIME2 NOT NULL DEFAULT GETDATE(),
    [picture] VARBINARY(MAX),
    CONSTRAINT [fk_staff_address] FOREIGN KEY ([address_id]) 
        REFERENCES [dbo].[address]([address_id]),
    CONSTRAINT [fk_staff_store] FOREIGN KEY ([store_id]) 
        REFERENCES [dbo].[store]([store_id])
);

-- Add foreign key from store to staff (circular reference handled after both tables exist)
ALTER TABLE [dbo].[store]
    ADD CONSTRAINT [fk_store_staff] FOREIGN KEY ([manager_staff_id]) 
        REFERENCES [dbo].[staff]([staff_id]);

ALTER TABLE [dbo].[store]
    ADD CONSTRAINT [fk_store_address] FOREIGN KEY ([address_id]) 
        REFERENCES [dbo].[address]([address_id]);

-- Inventory table
CREATE TABLE [dbo].[inventory] (
    [inventory_id] INT IDENTITY(1,1) PRIMARY KEY,
    [film_id] INT NOT NULL,
    [store_id] INT NOT NULL,
    [last_update] DATETIME2 NOT NULL DEFAULT GETDATE(),
    CONSTRAINT [fk_inventory_film] FOREIGN KEY ([film_id]) 
        REFERENCES [dbo].[film]([film_id]),
    CONSTRAINT [fk_inventory_store] FOREIGN KEY ([store_id]) 
        REFERENCES [dbo].[store]([store_id])
);

CREATE INDEX [idx_inventory_film_id] ON [dbo].[inventory]([film_id]);
CREATE INDEX [idx_inventory_store_id_film_id] ON [dbo].[inventory]([store_id], [film_id]);

-- Rental table
CREATE TABLE [dbo].[rental] (
    [rental_id] INT IDENTITY(1,1) PRIMARY KEY,
    [rental_date] DATETIME2 NOT NULL,
    [inventory_id] INT NOT NULL,
    [customer_id] INT NOT NULL,
    [return_date] DATETIME2,
    [staff_id] INT NOT NULL,
    [last_update] DATETIME2 NOT NULL DEFAULT GETDATE(),
    CONSTRAINT [fk_rental_inventory] FOREIGN KEY ([inventory_id]) 
        REFERENCES [dbo].[inventory]([inventory_id]),
    CONSTRAINT [fk_rental_customer] FOREIGN KEY ([customer_id]) 
        REFERENCES [dbo].[customer]([customer_id]),
    CONSTRAINT [fk_rental_staff] FOREIGN KEY ([staff_id]) 
        REFERENCES [dbo].[staff]([staff_id])
);

CREATE INDEX [idx_rental_inventory_id] ON [dbo].[rental]([inventory_id]);
CREATE INDEX [idx_rental_customer_id] ON [dbo].[rental]([customer_id]);
CREATE INDEX [idx_rental_staff_id] ON [dbo].[rental]([staff_id]);
CREATE UNIQUE INDEX [idx_rental_unique] ON [dbo].[rental]([rental_date], [inventory_id], [customer_id]);

-- Payment table
CREATE TABLE [dbo].[payment] (
    [payment_id] INT IDENTITY(1,1) PRIMARY KEY,
    [customer_id] INT NOT NULL,
    [staff_id] INT NOT NULL,
    [rental_id] INT,
    [amount] DECIMAL(5,2) NOT NULL,
    [payment_date] DATETIME2 NOT NULL,
    CONSTRAINT [fk_payment_customer] FOREIGN KEY ([customer_id]) 
        REFERENCES [dbo].[customer]([customer_id]),
    CONSTRAINT [fk_payment_staff] FOREIGN KEY ([staff_id]) 
        REFERENCES [dbo].[staff]([staff_id]),
    CONSTRAINT [fk_payment_rental] FOREIGN KEY ([rental_id]) 
        REFERENCES [dbo].[rental]([rental_id])
);

CREATE INDEX [idx_payment_customer_id] ON [dbo].[payment]([customer_id]);
CREATE INDEX [idx_payment_staff_id] ON [dbo].[payment]([staff_id]);
CREATE INDEX [idx_payment_rental_id] ON [dbo].[payment]([rental_id]);
