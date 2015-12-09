function mapper() {
    text = this.text;
    if (text) {
        var removePunc = text.toString().toLowerCase().replace(/[.,-\/#!$%\^&\*;:{}=\-_`~()]/g,"");
        var finalText = punctuationless.replace(/\s{2,}/g, " ");
        var arrWords = finalText.split(" ");

        for (var i = arrWords.length - 1; i >= 0; i--) {
            word = arrWords[i].trim();
            if (word) {
                emit(word, 1);
            }
        }
    }
};