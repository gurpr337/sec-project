"""add_sigma_table

Revision ID: b7c4d2f5a8e9
Revises: ac4fd5330181
Create Date: 2025-09-23 12:02:18.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b7c4d2f5a8e9'
down_revision: Union[str, None] = 'ac4fd5330181'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create sigma table
    op.create_table('sigma',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('data_point_id', sa.Integer(), nullable=True),
        sa.Column('metric', sa.String(), nullable=True),
        sa.Column('period_ended', sa.String(), nullable=True),
        sa.Column('value', sa.Float(), nullable=True),
        sa.Column('denomination', sa.String(), nullable=True),
        sa.Column('source_table_name', sa.String(), nullable=True),
        sa.Column('source_cell', sa.String(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.ForeignKeyConstraint(['data_point_id'], ['sec_app.data_points.id'], ),
        sa.PrimaryKeyConstraint('id'),
        schema='sec_app'
    )
    # Create index on data_point_id
    op.create_index(op.f('ix_sec_app_sigma_data_point_id'), 'sigma', ['data_point_id'], unique=False, schema='sec_app')


def downgrade() -> None:
    # Drop sigma table
    op.drop_index(op.f('ix_sec_app_sigma_data_point_id'), table_name='sigma', schema='sec_app')
    op.drop_table('sigma', schema='sec_app')
