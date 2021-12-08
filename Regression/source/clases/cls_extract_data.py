# -*- coding: utf-8 -*-
__author__ = "Hugo Franco, Roberto Arias"
__maintainer__ = "Asignatura Data Analytics"
__copyright__ = "Copyright 2021 - Asignatura Data Analytics"
__version__ = "0.0.4"

try:
    from pathlib import Path as p
    import pandas as pd
    import glob
    import os
    
    import shutil
    import pickle, time
    from datetime import timedelta
    
except Exception as exc:
            print('Module(s) {} are missing.:'.format(str(exc)))

#%%
class extract_data_mf(object):
    
    
    def __init__(self, path=None):
        self.path = path
        self.data = None
        self.lst_files = None
        self.dataset = None
        self.status = False
        self.count = 0
        self.start_time = time.time()
    
    
    def check_used_space(self,path):
        try:
            self.check_path(path)
            if self.dir_exist:
                total_size = 0
                
                #use the walk() method to navigate through directory tree
                for dirpath, dirnames, filenames in os.walk(path):
                    for i in filenames:
                        
                        #use join to concatenate all the components of path
                        f = os.path.join(dirpath, i)
                        
                        #use getsize to generate size in bytes and add it to the total size
                        total_size += os.path.getsize(f)
                
                self.bytes = total_size
                total_size = self.formatSize()
                print('Espacio usado por el destino: {}'.format(total_size))
            else:
                print('{}No es posible calcular el espacio utilizado.'.format(os.linesep))
                print('El directorio {} no existe.'.format(path))
        
        except Exception as exc:
            self.show_error(exc)
    
    
    def check_free_space(self,path_data):
        try:
            self.bytes = shutil.disk_usage(str(path_data))[2]
            free_space = self.formatSize()
            print('Espacio libre en disco: {}'.format(free_space))
        
        except Exception as exc:
            self.show_error(exc)
    
    
    def formatSize(self):
        try:
            bytes = float(self.bytes)
            kb = bytes / 1024
        except:
            return "Error"
        if kb >= 1024:
            M = kb / 1024
            if M >= 1024:
                G = M / 1024
                return "%.2fG" % (G)
            else:
                return "%.2fM" % (M)
        elif kb == 0:
            return 'Folder vacio'
        else:
            return "%.2fkb" % (kb)
    
    
    def check_path(self,path_check):
        '''
        Valida que exista el path

        Returns
        -------
        None.

        '''
        self.dir_exist = os.path.exists(path_check)
    
    
    def get_lst_files(self,path_data,tipo):
        '''
        Lista los archivo de un directorio segun el tipo de solicitado.

        Parameters
        ----------
        path_data : string
            Ruta del directorio que contiene los archivos.
        tipo : string
            Extensión o tipo de archivo.

        Returns
        -------
        None.

        '''
        try:
            self.lst_files = [f for f in glob.glob(str(path_data)+'/**/*.'+ tipo.lower(), recursive=True)]
            
        except Exception as exc:
            self.show_error(exc)
    
    
    def get_data_csv_chunk(self, the_path=None, chunksize=1000,dropCols = None):
        '''
        Parameters
        ----------
        the_path : TYPE
            DESCRIPTION.

        Returns
        -------
        None.

        '''

        try:
            self.data = pd.DataFrame([])
            list_of_dataframes = []
            
            for chunk in pd.read_csv(the_path, chunksize=chunksize):
                
                # Procesar chunk ---> aplique SU proceso. Por ejemplo invocar una funcion.
                list_of_dataframes.append(chunk)
            
            self.data = pd.concat(list_of_dataframes)
        except Exception as exc:
            self.show_error(exc)
    
    
    # Control de exceptions
    def show_error(self,ex):
        '''
        Captura el tipo de error, su description y localización.

        Parameters
        ----------
        ex : Object
            Exception generada por el sistema.

        Returns
        -------
        None.

        '''
        trace = []
        tb = ex.__traceback__
        while tb is not None:
            trace.append({
                          "filename": tb.tb_frame.f_code.co_filename,
                          "name": tb.tb_frame.f_code.co_name,
                          "lineno": tb.tb_lineno
                          })
            
            tb = tb.tb_next
            
        print('{}Something was wrong:'.format(os.linesep))
        print('---type:{}'.format(str(type(ex).__name__)))
        print('---message:{}'.format(str(type(ex))))
        print('---trace:{}'.format(str(trace)))
        self.status = False
    
    
    def list_files(self):
        '''
        Imprime en pantalla cada uno de los elementos contenidos en lst_files

        Returns
        -------
        None.

        '''
        try:
            for f in self.lst_files:
                child = os.path.splitext(os.path.basename(f))[0]
                print(child)
                
        except Exception as exc:
            self.show_error(exc)
    
    
    def save_df_(self,df,filename = None):
        try:
            if filename is not None:
               df.to_excel(filename , sheet_name = 'sheet', index=False)
            
        except Exception as exc:
            self.show_error(exc)
    
    
    def spendTime(self,strLabel):
        '''
        Determina el tiempo que gasta un proceso.
        '''
        t1 = timedelta(seconds = self.start_time)
        t2 = timedelta(seconds = time.time())
        print ('{}{}{}'.format(os.linesep,str(strLabel),self.strfdelta(abs(t1-t2))))
    
    def strfdelta(self,tdelta):
        '''
        Convierte milisegundos a intervalos superiores de tiempo.
        '''
        strd = '[{days}] days: '
        strh = '[{hours}] hours: '
        strm = '[{minutes}] minutes: '
        strs = '[{seconds}] seconds'
        
        fmt = strd + strh + strm + strs
        d = {"days": tdelta.days}
        d["hours"], rem = divmod(tdelta.seconds, 3600)
        d["minutes"], d["seconds"] = divmod(rem, 60)
        
        if d['days']<1:
            fmt = strh + strm + strs
        if d['days']<1 and d['hours']<1:
            fmt = strm + strs
        if d['days']<1 and d['hours']<1 and d['minutes']<1:
            fmt = strs
        
        delta = fmt.format(**d)
        return delta






