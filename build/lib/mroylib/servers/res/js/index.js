function JsonAction(url, tp, data, callback){
    $.ajax({
        type: tp,
        url: url,
        data: JSON.stringify(data),
        success: function( data ) {
            // console.log(data);

            if (callback != null){
                callback(data);
            }
        },
        dataType: 'json'
    });

}