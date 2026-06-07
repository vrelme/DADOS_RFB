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


class ETLPipelineError(AppError):
    """Erro controlado do pipeline ETL"""
    pass


class DatabaseOperationError(DatabaseError):
    """Erro de banco com orientação operacional"""

    def __init__(
        self,
        operation,
        table_name,
        user_message,
        original_error=None
    ):
        self.operation = operation
        self.table_name = table_name
        self.original_exception = original_error
        self.user_message = user_message
        self.original_error = str(original_error) if original_error else None

        super().__init__(
            f"{operation} em {table_name} falhou. {user_message}"
        )

    def __reduce__(self):
        return (
            self.__class__,
            (
                self.operation,
                self.table_name,
                self.user_message,
                self.original_error,
            ),
        )
