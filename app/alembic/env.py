from alembic import context


def run_migrations_online() -> None:
    """Run migrations using the connection passed via config.attributes."""
    connectable = context.config.attributes.get("connection")

    if connectable is None:
        raise RuntimeError("No connection provided in alembic config attributes.")

    context.configure(connection=connectable)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_offline() -> None:
    raise NotImplementedError("Offline migrations not supported.")


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
