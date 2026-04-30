"""add_is_type2_transformed_to_financial_table

Revision ID: ac4fd5330181
Revises: 1f54b4f22f9f
Create Date: 2025-09-22 21:21:45.635706

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ac4fd5330181'
down_revision: Union[str, None] = '1f54b4f22f9f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add is_type2_transformed column to financial_tables table
    op.add_column('financial_tables', sa.Column('is_type2_transformed', sa.Boolean(), default=False), schema='sec_app')


def downgrade() -> None:
    # Remove is_type2_transformed column from financial_tables table
    op.drop_column('financial_tables', 'is_type2_transformed', schema='sec_app')
