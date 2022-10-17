"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():

    with open("clusters_report.txt") as r:
            lineas = r.readlines()

            #Títulos columnas
            col = []
            col_2 =[]

            col.append(lineas[0].split('  '))
            col.append(lineas[1].split('  '))
            col[0] = list(filter(None,col[0]))
            col[1] = list(filter(None,col[1]))
            col[0].remove(" \n")
            col[1][1] = ("palabras clave")


            col_2.append(col[0][0].strip().lower())
            col_2.append(col[0][1].strip().lower() + " " + col[1][0].strip().lower())
            col_2.append(col[0][2].strip().lower() + " " + col[1][1].strip().lower())
            col_2.append(col[0][3].strip().lower())

            for i in range(4):
                col_2[i] = col_2[i].replace(" ","_")


            #Primera columna
            col_cluster = []

            for j in range(4, len(lineas)):
                col_cluster.append(lineas[j][3:5].strip())

            #Segunda columna
            col_cpc = []
            for j in range(4, len(lineas)):
                col_cpc.append(lineas[j][5:12].strip())

            #tercera columna
            col_ppc = []
            for j in range(4, len(lineas)):
                col_ppc.append(lineas[j][25:29].strip().replace(",","."))

            #cuarta columna
            col_tex = []
            for k in range(4,len(lineas)-1):
                lineas[k].replace(".","")
            for j in range(4,len(lineas)):
                col_tex.append(lineas[j][40:].strip().replace("  "," "))

            col1 = col_tex[0] + " " + col_tex[1] + " " + col_tex[2] + " " + col_tex[3]
            col2 = col_tex[5] + " " + col_tex[6] + " " + col_tex[7] + " " + col_tex[8] + " " + col_tex[9]
            col3 = col_tex[11] + " " + col_tex[12] + " " + col_tex[13]
            col4 = col_tex[15] + " " + col_tex[16] + " " + col_tex[17] + " " + col_tex[18]
            col5 = col_tex[20] + " " + col_tex[21] + " " + col_tex[22] + " " + col_tex[23]
            col6 = col_tex[25] + " " + col_tex[26] + " " + col_tex[27] + " " + col_tex[28]
            col7 = col_tex[30] + " " + col_tex[31] + " " + col_tex[32] + " " + col_tex[33]
            col8 = col_tex[35] + " " + col_tex[36] + " " + col_tex[37] + " " + col_tex[38]
            col9 = col_tex[40] + " " + col_tex[41] + " " + col_tex[42] + " " + col_tex[43]
            col10 = col_tex[45] + " " + col_tex[46] + " " + col_tex[47] + " " + col_tex[48]
            col11 = col_tex[50] + " " + col_tex[51]
            col12 = col_tex[53] + " " + col_tex[54] + " " + col_tex[55] + " " + col_tex[56] + " " + col_tex[57]
            col13 = col_tex[59] + " " + col_tex[60] + " " + col_tex[61] + " " + col_tex[62]

            coltxt = [col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13]
            lista_2 = []
            lista_2 = zip(list(map(int, list(filter(None,col_cluster)))), list(map(int, list(filter(None,col_cpc)))),list(map(float,list(filter(None,col_ppc)))),coltxt)
            lista_f = list(lista_2)
            df = pd.DataFrame(lista_f, columns = col_2)
            #list(map(int, list(filter(None,col_cpc))))

    return df
