function doGet_0(e) {
    let result = { data: 100, weight: 42 }
    return ContentService.createTextOutput(JSON.stringify(result))
        .setMimeType(ContentService.MimeType.JSON);
}


function doGet_3(e) {
    try {
        if (e) {
            var rez = "Порядок в " + new Date();
        } else {
            var rez = "Ошибка в " + new Date();
        }
    } catch (c) {
        var rez = "произошла ошибка; " + c
    }
    return ContentService.createTextOutput(rez);
}

function doGet_2(e) {
    var output = "Hello from Web App!"; // Ваш текст
    return ContentService.createTextOutput(output).setMimeType(ContentService.MimeType.TEXT);
}
