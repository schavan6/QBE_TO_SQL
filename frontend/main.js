var tableNameToCount = new Map();
var tableNames = [];
var tableNameToColumnNames = new Map();

//passes MySQL credentials to get list of tables as a result of query
function loginAndGetTables() {
	var that=this;
	var url = 'http://127.0.0.1:5000/qbe/?query={tables(username:"'+$('#username').val()+'", password:"'+ $('#password').val()+'", database:"'+ $('#database').val() +'"){tname}}';
    $.ajax({
    url: url,
    type: 'GET',
    success: function(response) {
		if(response.errors && response.errors.length != 0){
			alert(response.errors[0].message);
		}
		else{
			var tables = response.data.tables;
	    	that._createTableListHTML(tables);
		}
    },
    error: function(error) {
      alert("ERROR");
      console.log(error);
    }
  });
}
//creates list of tables with number input next to them : part 1
function _createTableListHTML(tables){
	var dropDownHTML = '';
	var htmlCode = '<table class="table"><tbody>';
	var i;
	var tableName;
	for(i=0; i<tables.length;i++){
		tableName = tables[i].tname;
		tableNames.push(tableName);
		htmlCode += '<tr><td>'+tableName+'</td><td><input name="count" class"form-control" type="number" id="'+tableName+'"min="0" max="2"></td>'
	}
	$('#tables').html(htmlCode);
	$('#database_tables').show();
	$('html, body').animate({
        scrollTop: $("#database_tables").offset().top
    }, 2000);

}
//passes table names user has selected to get their column details. Calls a function to construct skeletons
function getSkeletons(){
	this._resetSkeletons();
	var that=this;
	_fillTableNameTocount();
	var url;
	var i;
	var columns;
	if(tableNameToCount.size == 0){
		alert("Please select at least one table!")
	}
	else{

		tableNameToCount.forEach(function(value,key){
			url = 'http://localhost:5000/qbe/?query={columns(tablename:"'+key+'"){cname ctype tname}}';
		    $.ajax({
				url: url,
		   		type: 'GET',
			    success: function(response) {
					if(response.errors && response.errors.length != 0){
						alert(response.errors[0].message);
					}
					else{
						columns = response.data.columns;
						that._createAndAppendSkeleton(columns);  
					}
				 
			   },
			   error: function(error) {
				 alert("ERROR");
				 console.log(error);
			   }
			});
		});

	}
}
//From column names and data types received from the backend, constructs dynamic tables i.e. skeletons
function _createAndAppendSkeleton(columns){

	var tableName =columns[0].tname;
	var count = tableNameToCount.has(tableName) ? tableNameToCount.get(tableName) : 1;
	for(var j=1 ; j<= count ; j++){
		var skeletonHTML = '<table class="table table-bordered"><thead><tr><th><b>'+tableName.toUpperCase()+'</b></th>';
		for(var i=0;i<columns.length;i++){
			skeletonHTML+= '<th>' + columns[i].cname + '(' + columns[i].ctype + ')</th>';
		}
		skeletonHTML += '</tr></thead><tbody><tr><td><input class="form-control" type="text" id="'+tableName+'-'+j+'" name="colValue"></td>';
		for(i=0; i< columns.length;i++){
			if(!tableNameToColumnNames.has(tableName))
				tableNameToColumnNames.set(tableName,[]);
			var colList = tableNameToColumnNames.get(tableName);
			colList.push(columns[i].cname);
			tableNameToColumnNames.set(tableName,colList)
			skeletonHTML += '<td><input class="form-control" type="text" id="'+tableName+'-'+columns[i].cname+'-'+j+'" name="colValue"></td>';
		}
		skeletonHTML += '</tr></tbody></table><br>';
		$('#skeletons').append(skeletonHTML);
	}
	
	$('#qbe_interface').show();
	$('html, body').animate({
        scrollTop: $("#qbe_interface").offset().top
    }, 2000);

}


//sends qbe inputs to backend to get corrosponding SQL query and its results
function fetchQueryResults(){
	var that=this;
	var promise = this._validateOnFetchClick();

	promise.done(function(validated){
		if (!validated)
			return;

		var inputs = $('input[name="colValue"]');

		var idComponents;
		var columnEntry = {};
		var colList;
		var qbeInputs = [];
		var expr;
		var idsProcessed=[];
		var isColumnSelected = false;
		for(var i=0; i<inputs.length;i++){
			if(inputs[i].value.length > 0){
				if(inputs[i].value.indexOf('P.') > -1)
					isColumnSelected = true;

				idComponents = inputs[i].id.split("-")
				if(idComponents.length == 2 && inputs[i].value == 'P.'){
					
					colList = _.uniq(tableNameToColumnNames.get(idComponents[0]));
					for(var index in colList){
						columnEntry={};
						columnEntry['tableName'] = idComponents[0];
						columnEntry['columnName'] = colList[index];
						columnEntry['tableCardinality'] = idComponents[1];
						var actualColumnExpression = idComponents[0]+'-'+colList[index]+'-'+idComponents[1];
						expr = inputs[i].value;
						var actualExpression = $('#'+actualColumnExpression).val();
						if(actualExpression){
							if(actualExpression.startsWith('_')){
								expr = expr + $('#'+actualColumnExpression).val() ;
								idsProcessed.push(actualColumnExpression);
							}
						else if(!(actualExpression.startsWith('>=') || actualExpression.startsWith('<=') || actualExpression.startsWith('!=') || actualExpression.startsWith('>') || actualExpression.startsWith('<')))
							continue;

						}
						columnEntry['expression'] = expr;
						qbeInputs.push(columnEntry);
					}
		
				}
				else{
					if(!idsProcessed.indexOf(inputs[i].id) > -1){
						columnEntry={};
						columnEntry['tableName'] = idComponents[0];
						columnEntry['columnName'] = idComponents[1];
						columnEntry['tableCardinality'] = idComponents[2];
						columnEntry['expression'] = inputs[i].value;
						qbeInputs.push(columnEntry);
					}
				}
			}
		}

		if(!isColumnSelected){
			alert("Please select at least one column for the query to print.");
			return;
		}

		var url = 'http://localhost:5000/qbe/?query={qberesult(qbeconditions:[';

		var i=0;
		for (var i in qbeInputs){
			url += '{tableName:"' + qbeInputs[i].tableName + '",columnName:"' + qbeInputs[i].columnName + '",tableCardinality:"'+qbeInputs[i].tableCardinality;
			url += '",expression:"'+ qbeInputs[i].expression+'"}';
			if(i!=qbeInputs.length -1)
				url += ','
		}
		conditionBoxValue=$('#condition-box').val()
			url +='],conditionBox:"'+$('#condition-box').val()+ '"){values query}}';
		
			$.ajax({
			url: url,
			type: 'GET',
			success: function(response) {
			 

			    if(response.errors && response.errors.length != 0){
					alert("Wrong syntax. Please check your QBE conditions.");
				}
				else{
					that._showResults(response);
				}
			},
			error: function(error) {
			  alert("ERROR");
			  console.log(error);
			}
		});
	});
}

//validates the skeleton entries (QBE query) before making an ajax call for qbe to sql conversion
function _validateOnFetchClick(){
	var promise = $.Deferred();
	var that = this;
	tableNameToCount.forEach(function(count,tableName){

		var i;
		var expressionUnderTable;
		var validated = true;
		for(i=1;i<=count;i++){
			expressionUnderTable = $('#'+tableName+'-'+i).val();
			if(expressionUnderTable!= undefined && expressionUnderTable!="" && expressionUnderTable != 'P.'){
				alert("'P.' is the only acceptable expression under a table name.")
				validated = false;
			}
		}

		var conditionBoxExpresion = $('#condition-box').val();
		if(conditionBoxExpresion != undefined &&conditionBoxExpresion!="" && !(conditionBoxExpresion.indexOf('<') > -1 || conditionBoxExpresion.indexOf('>') > -1|| conditionBoxExpresion.indexOf('=') > -1 || conditionBoxExpresion.indexOf('AVG.') > -1)){
			alert("Condition box must contain a comparison or equality expression or an AVG operator.")
			validated = false;
		}

		promise.resolve(validated);

	});
	return promise;
}

//after qbe to sql conversion is done, this function creates dynamic tables to display results
function _showResults(response){
	
	
	$('#sql-query').text(response.data.qberesult.query);
	var resultColumns = response.data.qberesult.values;

	if(resultColumns.length > 1){
		var htmlString = '<table class="table table-bordered caption-bottom"><thead><tr>';
		for(var i in resultColumns[0]){
			htmlString += '<th>'+resultColumns[0][i] + '</th>';
		}
		htmlString += '</thead><tbody>';
		for(i=1;i<resultColumns.length;i++){
			htmlString += '<tr>';
			for(var j in resultColumns[i]){
				htmlString += '<td>' + resultColumns[i][j] + '</td>';
			}
			htmlString += '</tr>';
		}
		htmlString += '</tbody></table>';
		$('#results').html(htmlString);
	}
	else{
		$('#results').html("No results found.");
	}
	
	
	$('#qbe-result').show();
	
    $('html, body').animate({
        scrollTop: $("#qbe-result").offset().top
    }, 2000);


}
function _resetSkeletons(){
	tableNameToCount = new Map();
	$('#qbe_interface').hide();
	$('#qbe-result').hide();
	$('#condition-box').val("");
	this._removeDivChildren(document.getElementById('results'));
	this._removeDivChildren(document.getElementById('skeletons'));
}
function resetCount(){

	$('input[name="count"]').each(function() { 
		this.value = '';
	});
	this._resetSkeletons();
	
}
function _fillTableNameTocount(){
	tableNames.forEach(function(tableName,index){
		$('input[id="'+tableName+'"]').each(function() { 
			if(this.value > 0)
				tableNameToCount.set(tableName,this.value);
		});
	}); 
}
function _removeDivChildren(myNode){
	//node.hide();
	while (myNode.firstChild) {
    	myNode.removeChild(myNode.lastChild);
  }
}
