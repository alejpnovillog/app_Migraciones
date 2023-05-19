# We load the necessary libraries
try:
    from app_Abstract.gestionRegistros import GestionRegistros
    from app_Config.config import ConfigurarAplicacion
    from app_Conexion_Iseries_JtOpen.pythonJTOpen import JT400Helper
    import os
    import datetime
    from pydal import Field
    import unidecode
except Exception as e:
    print(f'Falta algun modulo Migracion Datos Relacion Arba Sucerp Marca {e}')


"""
SELECT * FROM GXTST.INFORMACIONVEHICULO a left outer join       
matanza.tmaut b on A."dominionuevo" = B.DACTUA WHERE B.DORIGI is
null                                                            
Se ha creado el archivo $$NOTMAUT en TATTA.                     






"""



class DatosRelacionArbaSucerpMarca():

    def __int__(self):
        pass