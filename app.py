from datetime import datetime
from flask import Flask, current_app, g, request, redirect, url_for
from flask.templating import render_template
import sqlite3
from flask_paginate import Pagination, get_page_args

app = Flask(__name__)
app.secret_key = '20211106_Fl4sK@P.'

#-------       Definir el tamaño máximo del archivo csv
app.config['MAX_CONTENT_PATH'] = 1000000



@app.route('/')
#-------       Cargar página por defecto
def default():
    #-------       Crear base de datos para guardar archivo csv [only data]
    con = sqlite3.connect('retoazure_01.db')
    cur = con.cursor()
    cur.execute("create table if not exists retoazure_01(fecha_carga varchar(30), hora_carga varchar(15),Field01 varchar(100),Field02 varchar(20),Field03 varchar(20),Field04 varchar(60),Field05 varchar(60),Field06 varchar(100),Field07 varchar(100),Field08 varchar(100),Field09 varchar(100),Field10 varchar(100),Field11 varchar(100),Field12 varchar(100),Field13 varchar(100),Field14 varchar(100),Field15 varchar(60),Field16 varchar(60),Field17 varchar(60),Field18 varchar(60),Field19 varchar(60),Field20 varchar(60));")
    con.commit()
    con.close()
    return render_template('index.html')

@app.route('/delete')
#-------       Cargar página por defecto
def delete():
    #-------       Crear base de datos para guardar archivo csv [only data]
    try:
        con = sqlite3.connect('retoazure_01.db')
        cur = con.cursor()
        cur.execute("drop table retoazure_01;")
        con.commit()
        con.close()
    except:
        pass
    return render_template('index.html')

@app.route('/Carga', methods=['GET', 'POST'])
#-------       funcion index para redireccionar a la pagina de upload
def Carga():
    if request.method == "POST":
        fFile = request.files['newfile']
        blob = fFile.read()
        length = len(blob)

        if length < app.config['MAX_CONTENT_PATH']:
            #fFile.save(secure_filename(fFile.filename))
            rsp = guardarArchivoEnBaseDeDatos(blob)
            return rsp
        else:
            return "Bad file size"
    else:
        return render_template('carga.html')

def guardarArchivoEnBaseDeDatos(file):
    con = sqlite3.connect('retoazure_01.db')
    cursave = con.cursor()
    arrFile = file.decode().split('\r\n')
    arrLog = []
    _tot = 1
    _sum = 0
    for _line in arrFile:
        try:
            _arrLn = _line.split(',')
            _val0 = _arrLn[0]
            _sql = "select * from retoazure_01 where Field01 = '{}'".format(_val0)
            cursave.execute(_sql)
            data = cursave.fetchall()
            if data != None and "Field" not in _val0:
                if len(data) > 0:
                    pass
                else:
                    _nowa = datetime.now()
                    _today = datetime.today()
                    _date_load = _today.strftime("%Y-%m-%d")
                    _hour_load = _today.strftime("%H:%M:%S")
                    _newLn = "'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'" .format(
                       _date_load, _hour_load, _arrLn[0],_arrLn[1],_arrLn[2],_arrLn[3],_arrLn[4],_arrLn[5],_arrLn[6],_arrLn[7],_arrLn[8],_arrLn[9],_arrLn[10],_arrLn[11],_arrLn[12],_arrLn[13],_arrLn[14],_arrLn[15],_arrLn[16],_arrLn[17],_arrLn[18],_arrLn[19])
                    _sql = "insert into retoazure_01 values({})".format(_newLn)
                    cursave.execute(_sql)
                    con.commit()
                    _nowb = datetime.now()
                    _nowres = _nowb - _nowa
                    _msgs = _nowres.microseconds
                    _sum = _sum + _msgs
                    _msgs = _msgs / 1000000
                    arrLog.append("Registro " + str(_tot) + " Ingresado en: " + str(_msgs) + " segundos")
                    _tot = _tot + 1

        except:
            pass

    con.close()

    _prom = _sum / _tot
    _prom = _prom / 1000
 
    return render_template('log.html', total = _tot, arr = arrLog, prom = int(_prom))
    #return render_template('carga.html')

@app.route('/Consulta', methods=['GET', 'POST'])
#-------       funcion para cargar el archivo
def Consulta():
    if request.method == "POST":
        key = request.form['keyword'].replace(" ", "").lower()
        value = request.form['valueword']

        con = sqlite3.connect('retoazure_01.db')
        cur = con.cursor()
        _nowa = datetime.now()
        
        _sql1 = "select * from retoazure_01 where upper({}) like upper('%{}%')".format(key.upper(), value)


        page, per_page, offset = get_page_args(
        page_parameter="p", per_page_parameter="pp", pp=20
        )
        
        if per_page:
            _sql = "select * from retoazure_01 where upper({}) like upper('%{}%') limit {}, {}".format(
                key.upper(), value, offset, per_page
            )
        else:
            _sql = _sql1

        cur.execute(_sql)
        data = cur.fetchall()
        _nowb = datetime.now()
        total = len(data)
        _nowres = _nowb - _nowa
        _segs = _nowres / 1000000
        _prom = _segs / len(data)

        pagination = get_pagination(
        p=page,
        pp=per_page,
        total=total,
        record_name="data",
        format_total=True,
        format_number=True,
        page_parameter="p",
        per_page_parameter="pp",
    )

        return render_template('maestroconsulta.html', records=data, pagination=pagination, segundos=_segs, prom = _prom)
    else:
        return render_template('buscar.html')

@app.route('/detalle', methods=['GET', 'POST'])
def detalle():
    if request.method == "POST":
        key = request.form['optradio']
        con = sqlite3.connect('retoazure_01.db')
        cur = con.cursor()
        _nowa = datetime.now()
        _sql = "select * from retoazure_01 where upper(Field01) = upper('{}')".format(key)
        cur.execute(_sql)
        data = cur.fetchall()
        _narr = []
        _newdata = []

        _newdata = list(data[0])

        if len(_newdata)>0:
            campo4 = _newdata[6]
            campo9 = _newdata[10]
            campo6 = _newdata[9]
            _dif = float(campo6)-float(campo4)
            try:
                calculo = (1 - float(campo4) * float(campo9))/(_dif)
            except:
                calculo = 0

            _narr.append(_newdata[0])
            _narr.append(_newdata[1])
            _narr.append(calculo)

            for i in range(2,len(_newdata)):
                _narr.append(_newdata[i])

            _nowb = datetime.now()
            _nowres = _nowb - _nowa
            _segs = _nowres / 1000000
            _prom = _segs / len(_newdata)

        else:
            _nowb = 0
            _nowres = 0
            _segs = 0
            _prom = 0

        return render_template('detalleconsulta.html', records=_narr, segundos=_segs, prom = _prom)
    else:
        return render_template('buscar.html')

def get_css_framework():
    css = request.args.get("bs")
    if css:
        return css

    return current_app.config.get("CSS_FRAMEWORK", "bootstrap4")


def get_link_size():
    return current_app.config.get("LINK_SIZE", "")


def get_alignment():
    return current_app.config.get("LINK_ALIGNMENT", "")


def show_single_page_or_not():
    return current_app.config.get("SHOW_SINGLE_PAGE", False)


def get_pagination(**kwargs):
    kwargs.setdefault("record_name", "records")
    return Pagination(
        css_framework=get_css_framework(),
        link_size=get_link_size(),
        alignment=get_alignment(),
        show_single_page=show_single_page_or_not(),
        **kwargs
    )

if __name__=="__main__":
    app.run(port=5500, debug=True)