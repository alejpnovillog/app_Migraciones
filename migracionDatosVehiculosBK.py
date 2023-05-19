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
    print(f'Falta algun modulo Migracion Datos Vehiculos {e}')


class DatosVehiculos():

    # builder
    def __init__(self, envdds, envsql, archivotexto, encoding ):

        """

            Esta clase es llamada por :
            Pocedimientos_Migracion_Tablas.lanzadorMigracionDatosVehiculos

            :param envdds:          It is the environment for tables in DDS format
            :param envsql:          It is the envoronment for tables in SQL format
            :param archivotexto:    is the name of the text file
            :param encoding:        It is the encoding code of the text file

        """

        # I get the process date and encoding
        self.fechaproceso = datetime.datetime.now()
        self.encoding = encoding


        # We define the data connection only for tables with DDS--------------
        self.data_Input_Dds = GestionRegistros(ambiente=envdds)
        self.schema_envdds = self.data_Input_Dds.__getattribute__('instancia_Host_Input_Dict')['schema']

        # we get the connection structure
        self.con = self.data_Input_Dds.__getattribute__('instancia_Host_Input_Dict')


        # we define the iseries connection to execute commands
        self.iprod = JT400Helper(self.con['ip'], self.con['usuario'], self.con['password'])


        # we define the data connection only for SQL tables----------------------
        self.data_Input = GestionRegistros(ambiente=envsql)
        self.schema_envsql = self.data_Input.__getattribute__('instancia_Host_Input_Dict')['schema']

        # we get the Table number and the dal_object from TABLA_TIPO_CUERPO -----------
        self.idTipoCuerpo = ConfigurarAplicacion.LISTA_TABLAS['TABLA_TIPO_CUERPO']['numero']
        self.objetoTipoCuerpo = self.data_Input.__getattribute__(ConfigurarAplicacion.LISTA_TABLAS['TABLA_TIPO_CUERPO']['objeto'])

        # we get the Table number and the dal_object from TABLA_TIPO_REGISTRO -----------
        self.idTipoRegistro = ConfigurarAplicacion.LISTA_TABLAS['TABLA_TIPO_REGISTRO']['numero']
        self.objetoTipoRegistro = self.data_Input.__getattribute__(ConfigurarAplicacion.LISTA_TABLAS['TABLA_TIPO_REGISTRO']['objeto'])

        # we get the Table number and the dal_object from TABLA_TIPO_SUB_REGISTRO -----------
        self.idTipoSubRegistro = ConfigurarAplicacion.LISTA_TABLAS['TABLA_TIPO_SUB_REGISTRO']['numero']
        self.objetoTipoSubRegistro = self.data_Input.__getattribute__(ConfigurarAplicacion.LISTA_TABLAS['TABLA_TIPO_SUB_REGISTRO']['objeto'])

        # we get the Table number and the dal_object from TABLA_TIPO_ORIGEN -----------
        self.idTipoSubRegistro = ConfigurarAplicacion.LISTA_TABLAS['TABLA_TIPO_ORIGEN']['numero']
        self.objetoTipoOrigen = self.data_Input.__getattribute__(ConfigurarAplicacion.LISTA_TABLAS['TABLA_TIPO_ORIGEN']['objeto'])

        # we get the Table number and the dal_object from TABLA_TIPO_DOCUMENTO' -----------
        self.idTipoDocumento = ConfigurarAplicacion.LISTA_TABLAS['TABLA_TIPO_DOCUMENTO']['numero']
        self.objetoTipoDocumento = self.data_Input.__getattribute__(ConfigurarAplicacion.LISTA_TABLAS['TABLA_TIPO_DOCUMENTO']['objeto'])

        # obtenemos el idTabla  y el objeto_dal de TABLA_PROVINCIA -----------
        self.idProvincia = ConfigurarAplicacion.LISTA_TABLAS['TABLA_PROVINCIA']['numero']
        self.objetoProvincia = self.data_Input.__getattribute__(ConfigurarAplicacion.LISTA_TABLAS['TABLA_PROVINCIA']['objeto'])

        # obtenemos el idTabla  y el objeto_dal de TABLA_ENCABEZADO -------------------
        self.idEncabezado = ConfigurarAplicacion.LISTA_TABLAS['TABLA_ENCABEZADO']['numero']
        self.objetoEncabezado = self.data_Input.__getattribute__(ConfigurarAplicacion.LISTA_TABLAS['TABLA_ENCABEZADO']['objeto'])

        # obtenemos el idTabla  y el objeto_dal de TABLA_TMPINFORMACIONVEHICULO -------------------
        self.idInfVehiculo = ConfigurarAplicacion.LISTA_TABLAS['TABLA_TMPINFORMACIONVEHICULO']['numero']
        self.objetoInfVehiculoTmp = self.data_Input_Dds.__getattribute__(ConfigurarAplicacion.LISTA_TABLAS['TABLA_TMPINFORMACIONVEHICULO']['objeto'])

        # obtenemos el idTabla  y el objeto_dal de TABLA_TMPINFORMACIONVEHICULOTITULAR -------------------
        self.idInfVehiculoTit = ConfigurarAplicacion.LISTA_TABLAS['TABLA_TMPINFORMACIONVEHICULOTITULAR']['numero']
        self.objetoInfVehiculoTitTmp = self.data_Input_Dds.__getattribute__(ConfigurarAplicacion.LISTA_TABLAS['TABLA_TMPINFORMACIONVEHICULOTITULAR']['objeto'])

        # obtenemos el idTabla  y el objeto_dal de TABLA_PIE -------------------
        self.idPie = ConfigurarAplicacion.LISTA_TABLAS['TABLA_PIE']['numero']
        self.objetoPie = self.data_Input.__getattribute__(ConfigurarAplicacion.LISTA_TABLAS['TABLA_PIE']['objeto'])

        # obtenemos el objeto_dal de TABLA_INFORMACIONVEHICULO -------------------
        self.objetoInfVehiculo = self.data_Input.__getattribute__(ConfigurarAplicacion.LISTA_TABLAS['TABLA_INFORMACIONVEHICULO']['objeto'])

        # obtenemos el idTabla  y el objeto_dal de TABLA_INFORMACIONVEHICULOTITULAR -------------------
        self.objetoInfVehiculoTit = self.data_Input.__getattribute__(ConfigurarAplicacion.LISTA_TABLAS['TABLA_INFORMACIONVEHICULOTITULAR']['objeto'])


        self.parm = {}


        self.insert_informacionvehiculo =''
        self.insert_informacionvehiculo += f'INSERT INTO {self.schema_envsql}.INFORMACIONVEHICULO( '
        self.insert_informacionvehiculo += 'TIPOR00001, TIPOS00001, CODIG00001, DOMIN00001, DOMIN00002, '
        self.insert_informacionvehiculo += 'CODIG00002, ORIGE00001, CATEG00001, MARCA00001, TIPOV00001, '
        self.insert_informacionvehiculo += 'MODEL00001, YYYYM00001, PESO_00001, CARGA00001, CILIN00001, '
        self.insert_informacionvehiculo += 'VALUA00001, CODIG00003, DESCR00001, FECHA00001, FECHA00002, '
        self.insert_informacionvehiculo += 'FECHA00003, ESTAD00001, FECHA00004, GUARD00001, CALLE00001, '
        self.insert_informacionvehiculo += 'NUMER00001, PISO_00001, DEPAR00001, BARRI00001, LOCAL00001, '
        self.insert_informacionvehiculo += 'CODIG00004, PROVI00001, CANTI00001, CODIG00005, RAZON00001, '
        self.insert_informacionvehiculo += 'FECHA00005, RESER00001, CONTR00001, KTIME00001) '
        self.insert_informacionvehiculo += 'SELECT TIPOR00001, TIPOS00001, CODIG0ORGA, DOMIN00001, DOMIN00002, '
        self.insert_informacionvehiculo += 'CODMTMFNM1, ORIGENID01, CATEG00001, MARCA00001, TIPOV00001, '
        self.insert_informacionvehiculo += 'MODEL00001, YYYYM00001, PESO_00001, CARGA00001, CILIN00001, '
        self.insert_informacionvehiculo += 'VALUA00001, CODIG00003, DESCR00001, FECHA00001, FECHA00002, '
        self.insert_informacionvehiculo += 'FECHA00003, ESTAD00001, FECHA00004, GUARD00001, CALLE00001, '
        self.insert_informacionvehiculo += 'NUMER00002, PISO_00001, DEPAR00001, BARRI00001, LOCAL00001, CODIG00004, '
        self.insert_informacionvehiculo += 'PROVI00001, CANTI00001, CODIG00005, RAZON00001, FECHA00005, RESER00001, '
        self.insert_informacionvehiculo += 'CONTR00001, KTIME00001 FROM epagos.infor00001'

        self.parm = {}

        self.insert_informacionvehiculoTit = ''
        self.insert_informacionvehiculoTit += f'INSERT INTO {self.schema_envsql}.INFORMACIONVEHICULOTITULAR(TIPOC00001, TIPOS00001, TIPOD00001, '
        self.insert_informacionvehiculoTit += 'NUMER00001, CUITC00001, APELL00001, PORCE00001, CALLE00001, NUMER00002, PISO_00001, '
        self.insert_informacionvehiculoTit += 'DEPAR00001, BARRI00001, LOCAL00001, CODIG00001, PROVI00001, RESER00001, INFVE00002, KTIME00001) '
        self.insert_informacionvehiculoTit += 'SELECT TIPOC00001, TIPOS00001, TIPOD00001, NUMER00001, CUITC00001, APELL00001, PORCE00001, '
        self.insert_informacionvehiculoTit += 'CALLE00001, NUMER00002, PISO_00001, DEPAR00001, BARRI00001, LOCAL00001, CODIG00001, '
        self.insert_informacionvehiculoTit += 'PROVI00001, RESER00001, INFVE00002, KTIME00001 FROM epagos.infor00002'




        # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        # Establecemos la ruta donde esta el archivo
        self.ruta = os.getcwd() + "\\Archivos_SinProcesar\\MigracionVehiculos"
        os.chdir(self.ruta)

        # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        # Establecemos el archivo de texto
        # archivo_texto = "2481cfcc14ab493c51b9a4d02567bac1FIJO.txt"
        self.archivo_texto = archivotexto

        # diccionario para la relacion entre titulares y vehiculos
        self.relacion = {'C': None, 'T': []}

        # realizamos el control de las tablas que vamos actualizar
        tablas = [
            {
                'lib': self.con['schema'],
                'file': self.data_Input_Dds.tmpInformacionVehiculo_Dal.sql_shortref,
                'src': 'QDDSSRC'
             },
            {
                'lib': self.con['schema'],
                'file': self.data_Input_Dds.tmpInformacionVehiculoTitular_Dal.sql_shortref,
                'src': 'QDDSSRC'
            }
        ]

        # realizamos el control del tratamiento de las tablas temporales
        self.msg = self.controlTablas(tablas, self.iprod)

        # reiniciamos el identity de  informacionvehiculo, informacionvehiculotitular
        self.reiniciar()

        # inicializamos los totales
        self.total = 0

        # obtenemos el ultimo id de la tabla INFORMACIONVEHICULO
        self.totalC = self.ultimoInfVehiculo()

        # obtenemos el ultimo id de la tabla INFORMACIONVEHICULOTITULAR
        self.totalT = self.ultimoInfVehiculoTitular()



    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # reiniciar el identity de las tablas
    def reiniciar(self):

        # obtenemos el nombre del schema
        con = self.data_Input.__getattribute__('instancia_Host_Input_Dict')
        lib = con['schema']

        # elimimar registros de la tabla informacionvehiculo
        print(f'Hacemos un DELETE de la tabla {lib}/informacionvehiculo ................ ')
        str = f'delete from {lib}.infor00001'
        respuesta = self.data_Input.run_comando(str, **self.parm)

        # realizamos un clear de informacionvehiculo
        print(f'Hacemos un CLRPFM  de la tabla {lib}/informacionvehiculo ................ ')
        str = f'CLRPFM FILE({lib}/INFOR00001)'

        # ejecutamos el comando
        msg = self.iprod.GetCmdMsg(str)

        #  verificamos si hay error
        if not msg[0]:
            return msg

        # reiniciamos el identity
        print(f'Reiniciamos el Identity en 1 del la tabla {lib}/informacionvehiculo  ................ ')
        str = f'alter table {lib}.infor00001 alter INFVE00001 restart with 1'
        respuesta = self.data_Input.run_comando(str, **self.parm)

        # elimimar registros de la tabla informacionvehiculotitular
        print(f'Hacemos un DELETE de la tabla {lib}/informacionvehiculotitular ................ ')
        str = f'delete from {lib}.infor00002'
        respuesta = self.data_Input.run_comando(str, **self.parm)

        # realizamos un clear de informacionvehiculotitular
        print(f'Hacemos un CLRPFM  de la tabla {lib}/informacionvehiculotitular ................ ')
        str = f'CLRPFM FILE({lib}/INFOR00002)'

        # ejecutamos el comando
        msg = self.iprod.GetCmdMsg(str)

        #  verificamos si hay error
        if not msg[0]:
            return msg

        # reiniciamos el identity
        print(f'Reiniciamos el Identity en 1 del la tabla {lib}/informacionvehiculotitular  ................ ')
        str = f'alter table {lib}.infor00002 alter INFVE00001 restart with 1'
        respuesta = self.data_Input.run_comando(str, **self.parm)


    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # obtenemos el ultimo id de la tabla INFORMACIONVEHICULO
    def ultimoInfVehiculo(self):
        self.objetoInfVehiculo = self.data_Input.__getattribute__(ConfigurarAplicacion.LISTA_TABLAS['TABLA_INFORMACIONVEHICULO']['objeto'])
        self.ultidInfVehiculo = self.data_Input.db().select(self.objetoInfVehiculo.infvehiculoid.max())

        if self.ultidInfVehiculo.response[0][0] == None:
            return 0
        else:
            return self.ultidInfVehiculo.response[0][0]

    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # obtenemos el ultimo id de la tabla INFORMACIONVEHICULOTITULAR
    def ultimoInfVehiculoTitular(self):
        self.objetoInfVehiculoTit = self.data_Input.__getattribute__(ConfigurarAplicacion.LISTA_TABLAS['TABLA_INFORMACIONVEHICULOTITULAR']['objeto'])
        self.ultidInfVehiculoTit = self.data_Input.db().select(self.objetoInfVehiculoTit.infvehiculotitularid.max())

        if self.ultidInfVehiculoTit.response[0][0] == None:
            return 0
        else:
            return self.ultidInfVehiculoTit.response[0][0]


    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # verificacion, creacion , limpieza de los registros de las tablas de la lista de tablas
    def controlTablas(self, tablas, iprod):
        """
        Objetivo:  Es verificar si las DDS existe
                   Es crear la tabla si las DDS existen
                   Es limpiar la tabla si la tabla existe
        tablas = Es una lista de las tablas controlar
                    Cada elemento de la lista es un diccionario con los siguientes elementos:
                    lib = Biblioteca
                    file = Es el nombre de la tabla
                    src = Es el archivo fuente donde dse encuentra la definicion del file

        iprod  = es el objeto de conexion a la iseries para ejecutar los comandos
        msg    = Es la respuesta de la ejecucion del comando donde
                 msg[0] Los valores posibles es True o False
                 msg[1] en adelante tenemos los mensajes que respondio el os400 ...
        return = msg
        """

        tablas = tablas

        # navegamos por el diccionario
        for elemento in tablas:

            lib = elemento["lib"]
            file = elemento["file"]
            src = elemento["src"]

            # verificamos si el fuente existe
            print(f'Verificamos SI EXISTE EL FUENTE  de la tabla {lib}/{file} .......................')
            str = f'CHKOBJ OBJ({lib}/QDDSSRC) OBJTYPE(*FILE) MBR({file})'

            # ejecutamos el comando
            msg = self.iprod.GetCmdMsg(str)

            #  si no existe el fuente
            if not msg[0]:

                return msg

            # si existe el fuente
            else:

                # verificamos si el objeto existe
                print(f'Verificamos SI EXISTE la tabla {lib}/{file} .......................')
                msg = self.iprod.CheckObjExists(lib, file, type="*FILE")

                # si no existe el objeto
                if not msg[0]:

                    # creamos la tabla
                    print(f'Creamos la tabla {lib}/{file} porque no existe ...................... ')
                    str = f'CRTPF FILE({elemento["lib"]}/{elemento["file"]}) SRCFILE({elemento["lib"]}/{elemento["src"]}) SIZE(*NOMAX)'

                    # creamos la tabla
                    msg = self.iprod.GetCmdMsg(str)

                    # si no hay error
                    if msg[0]:
                        print(f'Tabla {file} creada...')

                        # Registramos en el Journal
                        print(f'Registramos por Journal la tabla {lib}/{file} ...................... ')
                        str = f'STRJRNPF FILE({elemento["lib"]}/{elemento["file"]}) JRN({self.schema_envsql}/QSQJRN) IMAGES(*BOTH)'
                        # Registramos en el Journal
                        msg = self.iprod.GetCmdMsg(str)

                        # si no hay error
                        if msg[0]:
                            print(f'Tabla {file} Journalizada...')

                    # si hay error
                    else:
                        print(f'Tabla {lib}/{file} ERROR No se pudo crear...')
                        for error in msg:
                            print(error)

                # si exsite el objeto
                else:

                    # eliminamos los registros de la tabla
                    str = f'CLRPFM FILE({elemento["lib"]}/{elemento["file"]})'

                    # eliminamos registros de la tabla
                    msg = self.iprod.GetCmdMsg(str)

                    # si no hay error
                    if msg[0]:
                        print(f'Hacemos un clrpfm a la tabla {lib}/{file} ...................... ')

                     # si hay error
                    else:
                        print(f'Tabla {lib}/{file} ERROR No se pudo realizar el CLRPFM .......')
                        for error in msg:
                            print(error)
        return msg

    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # Read la tabla de Tipo de Registro
    def readTipoRegistro(self, **data):
        """
         data  = diccionario {'campo'': 'valor'}
                      campo = campo de la tabla a buscar
                      valor   = el valor del campo a buscar

         """
        # busco el registro en la tabla TIPOREGISTRO

        # Arma el query de busqueda para leer APIESTADOSTAREAS
        where = {
            'fieldnumber': [1, ],
            'field': [data['tiporegistro'], ],
            'struct_query': ['fld', ],
            'op': ['EQ', ],
            'order': False,
            'pageno': False,
            'indexpageno': False,
            'seleccion': False,
            'wrkrecords': False
        }


        data_list, errores = self.data_Input.get_rowsWhereWrk(self.objetoTipoRegistro, **where)

        # generamos una lista con los campos del registro
        data_list = [v for k, v in data_list.items()]

        # retornamos el valor del campo
        return data_list[0]['tiporegistroid']

    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # Read la tabla de Tipo de Sub Registro
    def readTipoSubRegistro(self, **data):
        """
         data  = diccionario {'campo'': 'valor'}
                      campo = campo de la tabla a buscar
                      valor   = el valor del campo a buscar

         """
        # busco el registro de la tabla TIPOSUBREGISTRO
        data_list, errores = self.data_Input.get_RowsWhere(self.objetoTipoSubRegistro, **data)

        # generamos una lista con los campos del registro
        data_list = [v for k, v in data_list.items()]

        # retornamos el valor del campo
        return data_list[0]['tiposubregistroid']

    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # Read la tabla de Tipo de Origen
    def readTipoOrigen(self, **data):
        """
         data  = diccionario {'campo'': 'valor'}
                      campo = campo de la tabla a buscar
                      valor   = el valor del campo a buscar

         """
        # busco el registro de la tabla TIPOORIGEN
        data_list, errores = self.data_Input.get_RowsWhere(self.objetoTipoOrigen, **data)

        # generamos una lista con los campos del registro
        data_list = [v for k, v in data_list.items()]

        # retornamos el valor del campo
        return data_list[0]['origenid']

    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # Read la tabla de Provincias
    def readProvincia(self, **data):

        """
         data  = diccionario {'campo'': 'valor'}
                      campo = campo de la tabla a buscar
                      valor   = el valor del campo a buscar

        """

        # verificamos si el dato reciobido es digitos
        if data['provincia'].isdigit():

            # asigno el campo con el el contenido en formato integer
            data['provincia'] = int(data['provincia'])

            # busco el registro de la tabla PROVINCIAS
            data_list, errores = self.data_Input.get_RowsWhere(self.objetoProvincia, **data)

            # generamos una lista con los campos del registro
            data_list = [v for k, v in data_list.items()]

            # retornamos el valor del campo
            return data_list[0]['provinciaid']

        # por error retorna un null
        else:
            return None

    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # Read la tabla de Tipo de Cuerpo
    def readTipoCuerpo(self, **data):

        """
         data  = diccionario {'campo'': 'valor'}
                      campo = campo de la tabla a buscar
                      valor   = el valor del campo a buscar

        """

        # busco el registro de la tabla TIPOCUERPO
        data_list, errores = self.data_Input.get_RowsWhere(self.objetoTipoCuerpo, **data)

        # generamos una lista con los campos del registro
        data_list = [v for k, v in data_list.items()]

        # retornamos el valor del campo
        return data_list[0]['tipocuerpoid']

    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # Read la tabla de Tipo de Documento
    def readTipoDocumento(self, **data):

        """
         data  = diccionario {'campo'': 'valor'}
                      campo = campo de la tabla a buscar
                      valor   = el valor del campo a buscar

        """

        # busco el registro de la tabla TIPODOCUMENTO
        data_list, errores = self.data_Input.get_RowsWhere(self.objetoTipoDocumento, **data)

        # generamos una lista con los campos del registro
        data_list = [v for k, v in data_list.items()]

        # retornamos el valor del campo
        return data_list[0]['tipodocumentoid']

    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # Tratamiento para la tabla ENCABEZADO
    def tipoRegistroE0(self, registro):
        """
         generamos un nuevo registro en la tabla ENCABEZADO

         registro = Es el registro leido del archivo de texto

         return = Es un lista
                  [0] = True o False
                  [1] en adelante tenemos un detalle del mensaje ...
        """
        try:

            # cargamos el diccionario del registro de la tabla ENCABEZADO
            insertE0 = dict()
            insertE0['tiporegistroid'] = self.readTipoRegistro(**{'tiporegistro': registro[0:2]})
            insertE0['versionprotocolo'] = registro[2:7]
            insertE0['revisionprotocolo'] = registro[7:12]
            insertE0['codigoorganismo'] = registro[12:20]
            insertE0['numeroenvio'] = registro[20:30]
            insertE0['fechahora'] = datetime.datetime.strptime(registro[31:45].strip(), '%Y%m%d%H%M%S')
            insertE0['ktimestamp'] = datetime.datetime.now()

            # write la tabla ENCABEZADO
            self.data_Input.campos.clear()
            respuesta = self.data_Input.add_Dal(self.objetoEncabezado, **insertE0)

            # determinamos la respuesta del insert
            if respuesta:
                return [True, f'El insert en la tabla ENCABEZADO fue satisfactoria, Id = {self.data_Input.ultimoid}']
            else:
                return [False, f'Error - {self.data_Input.ultimoerrorcapturado}']

        # captura del error
        except Exception as e:
            return [False, f'Error: {e} en la tabla ENCABEZADO']

    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # Tratamiento para la tabla INFORMACION DEL VEHICULO
    def tipoRegistroC5(self, id, registro):
        """
         generamos un nuevo registro en la tabla INFORMACIONVEHICULO

         id       = Es el id de la tabla
         registro = Es el registro leido del archivo de texto

         return = Es un lista
                  [0] = True o False
                  [1] en adelante tenemos un detalle del mensaje ...
        """

        try:

            # cargamos el diccionario del registro de la tabla INFORMACION DEL VEHICULO
            insertC5 = dict()

            # ------infvehiculoid--------------------------------------------------
            insertC5['INFVE00001'] = self.totalC

            # ------TIPO REGISTRO--------------------------------------------------
            insertC5['TIPOR00001'] = self.readTipoRegistro(**{'tiporegistro': registro[0:2]})

            # ------TIPO SUB REGISTRO--------------------------------------------------
            insertC5['TIPOS00001'] = self.readTipoSubRegistro(**{'tiposubregistro': registro[2:3]})

            # ------CODIGO ORGANISMO--------------------------------------------------
            insertC5['CODIG0ORGA'] = None
            if registro[3:11].lstrip().isdigit(): insertC5['CODIG0ORGA'] = int(registro[3:11])

            # -----DOMINIO NUEVO---------------------------------------------------
            insertC5['DOMIN00001'] = None
            if not registro[11:19].isspace(): insertC5['DOMIN00001'] = registro[11:19].lstrip()

            # -----DOMINIO VIEJO---------------------------------------------------
            insertC5['DOMIN00002'] = None
            if not registro[19:27].isspace(): insertC5['DOMIN00002'] = registro[19:27].lstrip()

            # ----CODIGO MTMFMM----------------------------------------------------
            insertC5['CODMTMFNM1'] = None
            if not registro[27:35].isspace(): insertC5['CODMTMFNM1'] = registro[27:35].lstrip()

            # ----TIPO ORIGEN----------------------------------------------------
            insertC5['ORIGENID01'] = self.readTipoOrigen(**{'tipoorigen': registro[35:36]})

            # ----CATEGORIA----------------------------------------------------
            insertC5['CATEG00001'] = None
            if not registro[36:39].isspace(): insertC5['CATEG00001'] = registro[36:39].lstrip()

            # ----MARCA----------------------------------------------------
            insertC5['MARCA00001'] = None
            if not registro[39:99].isspace(): insertC5['MARCA00001'] = registro[39:99].lstrip()

            # ----TIPO VEHICULO----------------------------------------------------
            insertC5['TIPOV00001'] = None
            if not registro[99:159].isspace(): insertC5['TIPOV00001'] = registro[99:159].lstrip()

            # ----MODELO----------------------------------------------------
            insertC5['MODEL00001'] = None
            if not registro[159:259].isspace(): insertC5['MODEL00001'] = unidecode.unidecode(registro[159:259].lstrip())

            # ---AÑO MODELO-----------------------------------------------------
            insertC5['YYYYM00001'] = None
            if registro[259:263].lstrip().isdigit(): insertC5['YYYYM00001'] = int(registro[259:263])

            # ---PESO--------------------------------------------------------
            insertC5['PESO_00001'] = None
            if registro[263:268].lstrip().isdigit(): insertC5['PESO_00001'] = int(registro[263:268])

            # ---CARGA--------------------------------------------------------
            insertC5['CARGA00001'] = None
            if registro[268:274].lstrip().isdigit(): insertC5['CARGA00001'] = int(registro[268:274])

            # ---CILINDRADA--------------------------------------------------------
            insertC5['CILIN00001'] = None
            if registro[274:279].lstrip().isdigit(): insertC5['CILIN00001'] = int(registro[274:279])

            # ---VALUACION--------------------------------------------------------
            insertC5['VALUA00001'] = None
            if registro[279:287].lstrip().isdigit(): insertC5['VALUA00001'] = int(registro[279:287])

            # ---CODIGO TIPO USO--------------------------------------------------------
            insertC5['CODIG00003'] = None
            if not registro[287:289].isspace(): insertC5['CODIG00003'] = registro[287:289].lstrip()

            # ---DESCRIPCION TIPO USO--------------------------------------------------------
            insertC5['DESCR00001'] = None
            if not registro[289:389].isspace(): insertC5['DESCR00001'] = unidecode.unidecode(registro[289:389].lstrip())

            # ---FECHA INSCRIPCION INICIAL--------------------------------------------------------
            insertC5['FECHA00001'] = None
            if registro[388:396].lstrip().isdigit():
                if not registro[388:396] == '00000000':
                    insertC5['FECHA00001'] = datetime.datetime.strptime(registro[388:396].strip(),
                                                                                     '%Y%m%d')

            # ----FECHA ULTIMA TRANSFERENCIA-------------------------------------------------------
            insertC5['FECHA00002'] = None
            if registro[396:404].lstrip().isdigit():
                if not registro[396:404] == '00000000':
                    insertC5['FECHA00002'] = datetime.datetime.strptime(registro[396:404].strip(),
                                                                                      '%Y%m%d')

            # ----FECHA ULTIMO MOVIMIENTO-------------------------------------------------------
            insertC5['FECHA00003'] = None
            if registro[404:412].lstrip().isdigit():
                if not registro[404:412] == '00000000':
                    insertC5['FECHA00003'] = datetime.datetime.strptime(registro[404:412].strip(), '%Y%m%d')

            # ----Estado Dominial-------------------------------------------------------
            insertC5['ESTAD00001'] = None
            if not registro[413:414].isspace(): insertC5['ESTAD00001'] = registro[413:414].lstrip()

            # ----FECHA CAMBIO ESTADO DOMINIAL-------------------------------------------------------
            insertC5['FECHA00004'] = None
            if registro[413:422].lstrip().isdigit():
                if not registro[413:422] == '00000000':
                    insertC5['FECHA00004'] = datetime.datetime.strptime(registro[413:422].strip(),
                                                                                      '%Y%m%d')

            # ----GUARDA HABITUAL-------------------------------------------------------
            insertC5['GUARD00001'] = None
            if not registro[422:423].isspace(): insertC5['GUARD00001'] = unidecode.unidecode(registro[422:423].lstrip())

            # ----CALLE-------------------------------------------------------
            insertC5['CALLE00001'] = None
            if not registro[423:463].isspace(): insertC5['CALLE00001'] = unidecode.unidecode(registro[423:463].lstrip())

            # ----NUMERO DE PUERTA-------------------------------------------------------
            insertC5['NUMER00002'] = None
            if not registro[463:473].isspace(): insertC5['NUMER00002'] = registro[463:473].lstrip()

            # ----PISO-------------------------------------------------------
            insertC5['PISO_00001'] = None
            if not registro[473:483].isspace(): insertC5['PISO_00001'] = unidecode.unidecode(registro[473:483].lstrip())

            # ----DEPARTAMENTO-------------------------------------------------------
            insertC5['DEPAR00001'] = None
            if not registro[483:493].isspace(): insertC5['DEPAR00001'] = unidecode.unidecode(registro[483:493].lstrip())

            # ----BARRIO-------------------------------------------------------
            insertC5['BARRI00001'] = None

            # ----LOCALIDAD-------------------------------------------------------
            insertC5['LOCAL00001'] = None
            if not registro[493:533].isspace(): insertC5['LOCAL00001'] = unidecode.unidecode(registro[493:533].lstrip())

            # ----CODIGO POSTAL-------------------------------------------------------
            insertC5['CODIG00004'] = None
            if not registro[533:541].isspace(): insertC5['CODIG00004'] = registro[533:541].lstrip()

            # ----PROVINCIA-------------------------------------------------------
            insertC5['PROVI00001'] = self.readProvincia(**{'provincia': registro[540:543]})

            # ----CANTIDAD TITULARES-------------------------------------------------------
            insertC5['CANTI00001'] = None
            if registro[543:545].lstrip().isdigit(): insertC5['CANTI00001'] = int(registro[543:545])

            # ----CODIGO REGISTRO SECCIONAL-------------------------------------------------------
            insertC5['CODIG00005'] = None
            if registro[545:550].lstrip().isdigit(): insertC5['CODIG00005'] = int(registro[545:550])

            # -----RAZON SOCIAL------------------------------------------------------
            insertC5['RAZON00001'] = None
            if not registro[550:590].isspace(): insertC5['RAZON00001'] = unidecode.unidecode(registro[550:590].lstrip())

            # -----FECHA OPERACION------------------------------------------------------
            insertC5['FECHA00005'] = datetime.datetime.strptime(registro[589:604].strip(), '%Y%m%d%H%M%S')

            # -----RESERVADO------------------------------------------------------
            insertC5['RESER00001'] = None
            if not registro[604:860].isspace():  insertC5['RESER00001'] = unidecode.unidecode(registro[604:860].lstrip())

            # -----TIMESTAMP------------------------------------------------------
            insertC5['KTIME00001'] = datetime.datetime.now()

            # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
            # write la tabla INFORMACION VEHICULO
            self.data_Input_Dds.campos.clear()
            respuesta = self.data_Input_Dds.add_Dal(self.objetoInfVehiculoTmp, **insertC5)


            # obtengo el ultimo id si la respuesta fue exitosa
            if respuesta:

                # asigno el id de la tabla de INFORMACIONVEHICULO
                if self.relacion['C'] == None: self.relacion['C'] = self.totalC

                return [True, f'El insert en la tabla INFORMACIONVEHICULO fue satisfactoria, Id = {self.totalC}']

            # hubo error
            else:
                return [False, f'Error - {self.data_Input.ultimoerrorcapturado}']

        # captura del error
        except Exception as e:
            return [False, f'Error: {e} en la tabla INFORMACIONVEHICULO']

    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # Tratamiento para la tabla INFORMACION DEL VEHICULO TITULAR
    def tipoRegistroC5Titular(self, id, registro):

        """
         generamos un nuevo registro en la tabla INFORMACIONVEHICULOTITULAR

         id       = Es el id de la tabla
         registro = Es el registro leido del archivo de texto

         return = Es un lista
                  [0] = True o False
                  [1] en adelante tenemos un detalle del mensaje ...
        """

        try:

            # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
            # cargamos el diccionario del registro de la tabla INFORMACION DEL VEHICULO TITULAR
            insertTitular = dict()

            insertTitular['INFVE00003'] = self.totalT

            # ----TIPO CUERPO-------------------------------------------------------
            insertTitular['TIPOC00001'] = self.readTipoCuerpo(**{'tipocuerpo': registro[0:2]})

            # ----TIPO SUB REGISTRO-------------------------------------------------------
            insertTitular['TIPOS00001'] = self.readTipoSubRegistro(**{'tiposubregistro': registro[2:3]})

            # ----TIPO DOCUMENTO-------------------------------------------------------
            insertTitular['TIPOD00001'] = self.readTipoDocumento(**{'tipodocumento': int(registro[3:5])})

            # ----NUMERO DOCUMENTO-------------------------------------------------------
            insertTitular['NUMER00001'] = None
            if registro[5:16].lstrip().isdigit(): insertTitular['NUMER00001'] = int(registro[5:16])

            # ----CUIT/CUIL-------------------------------------------------------
            insertTitular['CUITC00001'] = None
            if registro[16:27].lstrip().isdigit(): insertTitular['CUITC00001'] = int(registro[16:27])

            # ----APELLIDO NOMBRE-------------------------------------------------------
            insertTitular['APELL00001'] = None
            if not registro[27:177].isspace(): insertTitular['APELL00001'] = unidecode.unidecode(registro[27:177].lstrip())

            # ----PORCENTAJE TITULAR-------------------------------------------------------
            insertTitular['PORCE00001'] = None
            if registro[177:180].lstrip().isdigit(): insertTitular['PORCE00001'] = int(registro[177:180])

            # ----CALLE-------------------------------------------------------
            insertTitular['CALLE00001'] = None
            if not registro[180:220].isspace(): insertTitular['CALLE00001'] = unidecode.unidecode(registro[180:220].lstrip())[1:40]

            # -----NUMERO DE PUERTA------------------------------------------------------
            insertTitular['NUMER00002'] = None
            if not registro[220:230].isspace(): insertTitular['NUMER00002'] = registro[220:230].lstrip()

            # -----PISO------------------------------------------------------
            insertTitular['PISO_00001'] = None
            if not registro[230:240].isspace():  insertTitular['PISO_00001'] = unidecode.unidecode(registro[230:240].lstrip())[1:10]

            # -----DEPARTAMENTO------------------------------------------------------
            insertTitular['DEPAR00001'] = None
            if not registro[240:250].isspace(): insertTitular['DEPAR00001'] = unidecode.unidecode(registro[240:250].lstrip())[1:10]

            # -----BARRIO------------------------------------------------------
            insertTitular['BARRI00001'] = None
            if not registro[250:290].isspace(): insertTitular['BARRI00001'] = unidecode.unidecode(registro[250:290].lstrip())[1:40]

            # -----LOCALIDAD------------------------------------------------------
            insertTitular['LOCAL00001'] = None

            if not registro[290:330].isspace(): insertTitular['LOCAL00001'] = unidecode.unidecode(registro[290:330].lstrip())[1:40]


                # -----CODIGO POSTAL------------------------------------------------------
            insertTitular['CODIG00001'] = None
            if not registro[330:338].isspace(): insertTitular['CODIG00001'] = registro[330:338].lstrip()

            # -----PROVINCIA------------------------------------------------------
            insertTitular['PROVI00001'] = self.readProvincia(**{'provincia': registro[338:340]})

            # -----RESERVADO------------------------------------------------------
            insertTitular['RESER00001'] = None
            if not registro[340:596].isspace(): insertTitular['RESER00001'] = unidecode.unidecode(registro[340:596].lstrip())[1:40]

            # -----ID INFORMACION VEHICULO------------------------------------------------------
            insertTitular['INFVE00002'] = None

            # -----TIMESTAMP------------------------------------------------------
            insertTitular['KTIME00001'] = datetime.datetime.now()

            # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
            # write la tabla INFORMACION VEHICULO TITULAR

            self.data_Input_Dds.campos.clear()
            respuesta = self.data_Input_Dds.add_Dal(self.objetoInfVehiculoTitTmp, **insertTitular)

            # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
            # obtengo el ultimo id si la respuesta fue exitosa
            if respuesta:
                listaTitulares = list()
                listaTitulares = self.relacion['T']
                listaTitulares.append(self.totalT)
                self.relacion['T'] = listaTitulares
                return [True, f'El insert en la tabla INFORMACIONVEHICULOTIITULAR fue satisfactoria, Id = {self.totalT}']

            # hubo error
            else:
                return [False, f'Error - {self.data_Input.ultimoerrorcapturado}']

            return respuesta

        # captura del error
        except Exception as e:
            return [False, f'Error: {e} en la tabla INFORMACIONVEHICULOTITULAR']

    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # Tratamiento para la tabla PIE
    def tipoRegistroP0(self, registro):
        """
         generamos un nuevo registro en la tabla PIE

         registro = Es el registro leido del archivo de texto

         return = Es un lista
                  [0] = True o False
                  [1] en adelante tenemos un detalle del mensaje ...
        """

        try:

            # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
            # cargamos el diccionario del registro de la tabla PIE
            insertPie = dict()

            # ----TIPO REGISTRO-------------------------------------------------------
            insertPie['tiporegistroid'] = self.readTipoRegistro(**{'tiporegistro': registro[0:2]})

            # ----CANTIDAD DE REGISTROS-------------------------------------------------------
            insertPie['cantidadregistros'] = None
            if registro[2:10].lstrip().isdigit(): insertPie['cantidadregistros'] = int(registro[2:10])

            # ----NOMBRE DE ARCHIVO-------------------------------------------------------
            insertPie['checksum'] = None
            if not registro[10:42].isspace(): insertPie['checksum'] = registro[10:42]

            # -----Tiemstamp------------------------------------------------------
            insertPie['ktimestamp'] = datetime.datetime.now()

            # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
            # write la tabla PIE
            self.data_Input.campos.clear()
            respuesta = self.data_Input.add_Dal(self.objetoPie, **insertPie)

            # determinamos la respuesta del insert
            if respuesta:
                return [True, f'El insert en la tabla ENCABEZADO fue satisfactoria, Id = {self.data_Input.ultimoid}']
            else:
                return [False, f'Error - {self.data_Input.ultimoerrorcapturado}']

        # captura del error
        except Exception as e:
            return [False, f'Error: {e} en la tabla PIE']

    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # actualiza el titular en la tabla informacion vehiculo
    def actualizaTitular(self):

        # navegamos dentro de la lista de titulares para asignar el vehiculo
        for key in self.relacion['T']:

            # actualizar la tabla INFORMACION VEHICULO TITULAR
            data = {'INFVE00002': self.relacion['C']}

            id = self.relacion['T']

            respuesta = self.data_Input_Dds.upd_Dal(self.objetoInfVehiculoTitTmp, key, **data)

            # determinamos la respuesta del insert
            if not respuesta:
                return [False, f'Error Actualizacion - Id Titular = {key} Vehiculo = {data}']

        self.relacion['C'] = None
        self.relacion['T'] = []
        return [True]

    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # procesamiento del archivo recibido
    def procesamiento(self):

        try:
            # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
            # Open el archivo archivo de texto
            with open(self.archivo_texto, mode='r', encoding=self.encoding) as archivo:

                # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                # read el archivo de texto
                for linea in archivo:

                    # acumulo la cantidad de registro
                    self.total += 1


                    if self.total == 1:
                        self.ultvisualizar = self.total

                        # visualiza progreso del proceso
                        print(f'Se ha procesado {self.total} registros......')

                    if ((self.total - self.ultvisualizar) == 10000):
                        self.ultvisualizar = self.total

                        # visualiza progreso del proceso
                        print(f'Se ha procesado {self.total} registros......')


                    # asigno el valor de la linea del archivo de texto leido
                    registro = linea

                    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                    # actualizo la relacion entre titulares y los vehiculos
                    #if registro[2:3] == 'C':

                    #    if self.relacion['C'] != None and self.relacion['T'].__len__() != 0:

                            # actualizamos los titulares
                    #        msg = self.actualizaTitular()

                            # verifica si hubo error al actualizar
                    #3        if not msg[0]:
                    #            print(msg[1])
                    #            break

                    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                    # generamos el registro cabecera
                    if registro[0:2] == 'E0':

                        # Tratamiento de la cabecera
                        msg = self.tipoRegistroE0(registro)

                        # verifica si hubo error al grabar la cabecera
                        if not msg[0]:
                            print(msg[1])
                            break
                        else:
                            continue

                    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                    # generamos el registro de vehiculos
                    if registro[0:2] == 'C5' and registro[2:3] == 'C':

                        # verifica el corte de control
                        if self.relacion['C'] != None and self.relacion['T'].__len__() != 0:

                            # actualizamos los titulares
                            msg = self.actualizaTitular()

                            # verifica si hubo error al actualizar
                            if not msg[0]:
                                print(msg[1])
                                break



                        # Tratamiento de la informacion del vehiculo automoviles
                        self.totalC += 1
                        msg = self.tipoRegistroC5(self.totalC, registro)

                        # verifica si hubo error al grabar la informacionvehiculo
                        if not msg[0]:
                            print(msg[1])
                            break
                        else:
                            continue

                    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                    # generamos el registro de titular
                    if registro[0:2] == 'C5' and registro[2:3] == 'T':

                        # Tratamiento de la informacion del vehiculo automoviles
                        self.totalT += 1
                        msg = self.tipoRegistroC5Titular(self.totalT, registro)

                        # verifica si hubo error al grabar la informacionvehiculotitular
                        if not msg[0]:
                            print(msg[1])
                            break
                        else:
                            continue

                    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                    # generamos el registro de pie
                    if registro[0:2] == 'P0':

                        # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                        # actualizo la relacion de de los titulares y los vehiculos
                        if self.relacion['C'] != None and self.relacion['T'].__len__() != 0:


                            # actualizamos los titulares
                            msg = self.actualizaTitular()

                            # verifica si hubo error al actualizar
                            if not msg[0]:
                                print(msg[1])
                                break


                        # Tratamiento de la informacion de la tabla pie
                        msg = self.tipoRegistroP0(registro)

                        # verifica si hubo error al grabar el pie
                        if not msg[0]:
                            print(msg[1])
                            break
                        else:
                            continue

                # proceso completado
                print(f'Impactamos INFOR00001 de epagos a {self.schema_envsql}/informacionvehiculo ........')
                respuesta = self.data_Input.run_comando(self.insert_informacionvehiculo, **self.parm)
                print(f'Control error ---{respuesta}')

                print(f'Impactamos INFOR00002 de epagos a {self.schema_envsql}/informacionvehiculotitular ........')
                respuesta = self.data_Input.run_comando(self.insert_informacionvehiculoTit, **self.parm)
                print(f'Control error ---{respuesta}')

                print(f'proceso completado....total C = {self.totalC}    total T = {self.totalT} total leidos {self.total}')

        except Exception as e:
            print(f'total {self.total} - Error en el tronco del procedimiento {e}')



