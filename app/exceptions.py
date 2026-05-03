class AppError(Exception):
    """Erro base da aplicação"""
    pass


class DuplicateEntityError(AppError):
    """Registro duplicado"""
    pass


class NotFoundError(AppError):
    """Registro não encontrado"""
    pass


class DatabaseError(AppError):
    """Erro genérico de banco"""
    pass


class ValidationError(AppError):
    """Erro de validação"""
    pass