"""
src/repositories/base_repository.py
=========================================================
Repository genérico para TODAS as tabelas do projeto.

Padrão Enterprise:
- CRUD reutilizável
- Bulk insert
- Busca por filtros
- Delete
- Update
- Count

Compatível com:
Pais
Munic
Quals
Natju
Cnae
Empresa
Estabelecimento
Socios
Simples
=========================================================
"""

from typing import Type, List, Optional
from sqlalchemy.orm import Session


class BaseRepository:
    """
    Repository base reutilizável.
    """

    def __init__(self, session: Session, model: Type):
        self.session = session
        self.model = model

    # ==================================================
    # CREATE
    # ==================================================

    def add(self, **kwargs):
        obj = self.model(**kwargs)
        self.session.add(obj)
        return obj

    def bulk_insert(self, objects: List[dict]):
        """
        Insert em lote.
        """
        rows = [self.model(**item) for item in objects]
        self.session.bulk_save_objects(rows)

    # ==================================================
    # READ
    # ==================================================

    def get_all(self):
        return self.session.query(self.model).all()

    def get_by_id(self, pk):
        return self.session.get(self.model, pk)

    def filter_by(self, **kwargs):
        return self.session.query(self.model).filter_by(**kwargs).all()

    def first_by(self, **kwargs):
        return self.session.query(self.model).filter_by(**kwargs).first()

    def count(self):
        return self.session.query(self.model).count()

    # ==================================================
    # UPDATE
    # ==================================================

    def update(self, pk, **kwargs):
        obj = self.get_by_id(pk)

        if not obj:
            return None

        for key, value in kwargs.items():
            setattr(obj, key, value)

        return obj

    # ==================================================
    # DELETE
    # ==================================================

    def delete(self, pk):
        obj = self.get_by_id(pk)

        if obj:
            self.session.delete(obj)

    def delete_all(self):
        self.session.query(self.model).delete()

    # ==================================================
    # TRANSACTION
    # ==================================================

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()