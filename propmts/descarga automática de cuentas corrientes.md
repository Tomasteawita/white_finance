# Web Scraping para descargar cuentas corrientes de clientes de Balanz
Objetivo: Hacer la descarga automática de la cuenta corrientes de mis clientes a partir de una fecha en particular hasta el día de hoy con web scraping para la cuenta corriente de cada uno de mis clientes que se encuentran en data\balanz.
Contexto: El paso a paso para hacer el informe para un clientes es el siguiente:
1. Entro al link https://productores.balanz.com/
2. Pongo mi usuario y hago click en ingresar elemento:
```html
<input type="text" class="form-control login-username" name="username" value="" placeholder="Ingrese su nombre de Usuario">
```
y clickeo en el elemento 
```html
<a id="btn-login" class="btn btn-primary" data-loading-text="Loading..." onclick="Core.Business.Login.authenticate();">Ingresar</a>
```
3. Pongo mi contraseña y hago click en ingresar elemento:
```html
<input id="login-password" type="password" class="form-control login-password" name="password" placeholder="Ingrese su Clave">
```
y luego click en el elemento
```html
<a id="btn-login" class="btn btn-primary" data-loading-text="Loading..." onclick="Core.Business.Login.authenticate();">Ingresar</a>
```
4. Al entrar al menú principal ingreso el nombre de un cliente dentro del elemento 
```html
<input type="text" placeholder="" class="textbox-search">
```
en formato .title() y hago click en el elemento 
```html
<a style="margin-bottom:10px;" data-toggle="modal" class="btn btn-primary btn-search" title="Buscar">Buscar</a>
```
5. Hago click en el elemento 
```html
<button type="button" title="Acceder" class="btn-sm balanz-button"><span class="glyphicon glyphicon-resize-full"></span></button>
```
para entrar a la posición consolidada del cliente.
6. Posteriormente en todo este elemento:
```html
<ul class="nav nav-pills nav-main" id="topMain">
    <li class="dropdown">
        <a class="dropdown-toggle">MI CUENTA</a>
        <ul class="dropdown-menu">
            <li class="dropdown"><a>POSICION CONSOLIDADA</a></li>
            <li class="dropdown"><a>MOVIMIENTOS</a></li>
            <li class="dropdown"><a>CUENTA CORRIENTE</a></li>
            <li class="dropdown"><a>MOVIMIENTOS BANCARIOS</a></li>
            <li class="dropdown"><a>BOLETOS</a></li>
            <li class="dropdown"><a>ORDENES</a></li>
            <li class="dropdown"><a>PRECARGAS</a></li>
            <li class="dropdown"><a>CAUCIONES</a></li>
            <li class="dropdown"><a>RESUMEN MENSUAL</a></li>
        </ul>
    </li>
    <li class="dropdown">
        <a class="dropdown-toggle">RESEARCH</a>
        <ul class="dropdown-menu">
            <li class="dropdown"><a>SEMAFORO DE INVERSIONES</a></li>
            <li class="dropdown"><a>REPORTES</a></li>
            <li class="dropdown"><a>PODCASTS</a></li>
        </ul>
    </li>
    <li class="dropdown">
        <a class="dropdown-toggle">EVOLUCION</a>
        <ul class="dropdown-menu">
            <li class="dropdown"><a>EN PESOS</a></li>
            <li class="dropdown"><a>EN DOLARES</a></li>
            <li class="dropdown"><a>FLUJOS PROYECTADOS</a></li>
            <li class="dropdown"><a>RESULTADOS</a></li>
        </ul>
    </li>
    <li class="dropdown">
        <a class="dropdown-toggle">OPERAR</a>
        <ul class="dropdown-menu">
            <li class="dropdown"><a>ACCIONES</a></li>
            <li class="dropdown"><a>BONOS</a></li>
            <li class="dropdown"><a>FONDOS</a></li>
            <li class="dropdown"><a>CEDEARS</a></li>
            <li class="dropdown"><a>LETRAS</a></li>
            <li class="dropdown"><a>CORPORATIVOS</a></li>
            <li class="dropdown"><a>LICITACIONES</a></li>
            <li class="dropdown"><a>OPCIONES</a></li>
            <li class="dropdown"><a>FUTUROS</a></li>
            <li class="dropdown"><a>CAUCIONES</a></li>
            <li class="dropdown"><a>INFORME DE DEPOSITOS</a></li>
        </ul>
    </li>
    <li class="dropdown">
        <a class="dropdown-toggle">COTIZACIONES</a>
        <ul class="dropdown-menu">
            <li class="dropdown"><a>INVESTMENT IDEAS</a></li>
            <li class="dropdown"><a>ACCIONES</a></li>
            <li class="dropdown"><a>BONOS</a></li>
            <li class="dropdown"><a>CEDEARS</a></li>
            <li class="dropdown"><a>INTERNACIONAL</a></li>
            <li class="dropdown"><a>LETRAS</a></li>
            <li class="dropdown"><a>CORPORATIVOS</a></li>
            <li class="dropdown"><a>OPCIONES</a></li>
            <li class="dropdown"><a>FUTUROS</a></li>
            <li class="dropdown"><a>FONDOS</a></li>
            <li class="dropdown"><a>CAUCIONES</a></li>
            <li class="dropdown"><a>REAL ESTATE</a></li>
            <li class="dropdown"><a>LICITACIONES</a></li>
            <li class="dropdown"><a>TAGS</a></li>
            <li class="dropdown"><a>BUSCADOR</a></li>
        </ul>
    </li>
    <li class="dropdown">
        <a class="dropdown-toggle">INVESTMENT IDEAS <span style="margin-left: 10px;display: inline-block !important;">
            <img src="https://cms.balanz.com/PFS/055865_lightbulb.svg" style="width: auto; height: 16px;"></span></a>
    </li>
    <li class="dropdown"><a>WATCHLISTS</a></li>
    <li class="dropdown"><a>ALERTAS</a></li>
    <li class="dropdown"><a>GESTIONES</a></li>
</ul>
```
Clickeo en el elemento
```html
<a>CUENTA CORRIENTE</a>
```
7. Eso me hace entrar a la interface de cuenta corriente de mi cliente, en la cual interactuo con el siguiente elemento;
```html
<section class="cuenta">
        <div class="container">
            <div class="row">
                <div class="col-lg-12">
                    <div class="heading-title">
                        <h1 class="col-md-8 col-sm-6 col-xs-12">Cuenta Corriente</h1>
                        <div class="col-md-2 col-sm-6 col-xs-12 box-fecha pad0">
                            <span class="sui-datepicker"><input class="sui-picker-input" type="text"><span class="sui-icon-wrapper" unselectable="on" title=""><span class="sui-sprite sui-calendar-icon" unselectable="on">select</span></span><div class="col-md-12 col-sm-12 col-xs-12 pad0 date-picker-from" style="display: none;"></div></span>
                        </div>
                        <div class="col-md-2 col-sm-6 col-xs-12 box-fecha pad0">
                            <span class="sui-datepicker"><input class="sui-picker-input" type="text"><span class="sui-icon-wrapper" unselectable="on" title=""><span class="sui-sprite sui-calendar-icon" unselectable="on">select</span></span><div class="col-md-12 col-sm-12 col-xs-12 pad0 date-picker-to" style="display: none;"></div></span>
                        </div>
                    </div>
                </div>
                <div class="col-md-12 col-sm-12 col-xs-12 cuenta-tabla">
                    <label>Filtrar:</label> <input type="text" placeholder="" class="textbox-filter">
                    <label for="porLiquidacion">Mostrar por liquidación</label> <input id="porLiquidacion" type="checkbox" class="checkbox-por-liquidacion">
                    <div class="heading-table bgGris3 blanco radiusTop pad0" style="height:30px;">
                        <div class="col-md-12 col-sm-12 col-xs-12">
                            <span class="title label-title">Entre el 16/09/2026 al 17/10/2026</span>
                        </div>
                    </div>
                    <div class="col-md-12 col-sm-12 col-xs-12 radius shadow bgBlanco pad0 padding-top:30px">
                        <!--grid-->
                        <div class="div-cuenta-corriente sui-grid sui-grid-core"><div class="sui-group-panel"><div class="sui-group-panel-indicator" data-field="moneda"><span class="sui-ascending">▲</span><span class="sui-group-title">moneda</span></div></div><div class="sui-toolbar"><button type="button" class="sui-button">Exportar a Excel</button></div><div class="sui-gridheader"><table class="sui-table"><colgroup><col class="sui-indent-col"><col style="width:15%"><col style="width:40%"><col style="width:15%"><col style="width:15%"><col style="width:15%"></colgroup><thead><tr class="sui-columnheader"><th class="sui-indent-cell"></th><th class="blzGrid-column-header-even sui-headercell" data-field="fecha">Fecha</th><th class="blzGrid-column-header-odd sui-headercell" data-field="descripcion">Descripción</th><th class="blzGrid-column-header-even sui-headercell" data-field="cantidad">Cantidad</th><th class="blzGrid-column-header-odd sui-headercell" data-field="importe">Importe</th><th class="blzGrid-column-header-even sui-headercell" data-field="saldo">Saldo</th></tr></thead><tbody class="sui-hide"><tr><td></td><td></td><td></td><td></td><td></td></tr></tbody></table></div><div class="sui-gridcontent"><table class="sui-table sui-grouped-table"><colgroup><col class="sui-indent-col"><col style="width:15%"><col style="width:40%"><col style="width:15%"><col style="width:15%"><col style="width:15%"></colgroup><tbody><tr class="sui-group-header" data-group-level="1"><td class="sui-group-header-cell" colspan="6"><span class="sui-collapse">▾</span><span class="sui-group-header-text">moneda: Pesos</span></td></tr><tr class="sui-row" data-group-level="1"><td class="sui-indent-cell sui-group-intend-cell"></td><td style="text-align: left; padding-left: 5px;" class="sui-cell">2026-10-09</td><td style="text-align: left; padding-left: 5px;" class="sui-cell">Liquidación de Suscripción / 2160387 / BALANZ PERFORMANCE III clase A</td><td style="text-align: right; padding-right: 5px;" class="sui-cell">0</td><td style="text-align: right; padding-right: 5px;" class="sui-cell">-400,000.00</td><td style="text-align: right; padding-right: 5px;" class="sui-cell">218.23</td></tr><tr class="sui-alt-row" data-group-level="1"><td class="sui-indent-cell sui-group-intend-cell"></td><td style="text-align: left; padding-left: 5px;" class="sui-cell">2026-10-07</td><td style="text-align: left; padding-left: 5px;" class="sui-cell">Recibo de Cobro / 2769280</td><td style="text-align: right; padding-right: 5px;" class="sui-cell">0</td><td style="text-align: right; padding-right: 5px;" class="sui-cell">400,000.00</td><td style="text-align: right; padding-right: 5px;" class="sui-cell">400,218.23</td></tr></tbody></table></div><div class="sui-pager sui-pager-core"><ul class="sui-pagination"><li class="sui-pager-element sui-disabled"><a title="Go to the first page" data-page="1" class="sui-first">«</a></li><li class="sui-pager-element sui-disabled"><a title="Go to the previous page" data-page="0" class="sui-prev">‹</a></li><li class="sui-pager-element"><a data-page="1" class="sui-pager-number sui-selected">1</a></li><li class="sui-pager-element sui-disabled"><a title="Go to the next page" data-page="2" class="sui-next">›</a></li><li class="sui-pager-element sui-disabled"><a title="Go to the last page" data-page="1" class="sui-last">»</a></li></ul><div class="sui-pager-info-box">1 - 2 of 2 items</div></div></div>
                    </div>
                </div>
                <div class="col-md-12 col-sm-12 col-xs-12 mB15">
                    <p class="small">S.E.U.O. (Salvo Error U Omisión)</p>
                </div>
            </div>
        </div>
    </section>
```
En el cual, en el elemento 
```html
<input class="sui-picker-input" type="text">
```
Elijo la fecha desde que quiero descargar la cuenta corriente y en el elemento
```html
<button type="button" class="sui-button">Exportar a Excel</button>
```
Exporto a Excel.

Tarea: Este paso a paso lo hago con cada cliente, eso descarga un excel al directorio de descargas que siempte tiene el nombre "Cuenta Corriente.xlsx", a ese excel le agrego "-YYYYMMDD" antes del "." y despues de "Corriente", posteriormente muevo ese excel a la carpeta correspondiente de cada cliente, por ejemplo para el caso de Santiago Villafañe data\balanz\VILLAFAÑE SANTIAGO\Cuenta Corriente\

Quiero una forma de automatizar eso con web scraping, de manera que con un click pueda hacer una descarga desde la fecha que yo quiera para todos los clientes. Despues queda todos lo demás para hacer el informe pero la parte más laboriosa es esta, necesito una forma de automatizar ese paso a paso con webscraping. Mi navegador principal es google chrome.