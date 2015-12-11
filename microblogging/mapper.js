function mapper() {
    text = this.text;
    if (text) {
        var removePunc = text.toString().toLowerCase().replace(/[.,-\/#!$%\^&\*;:{}=\-_`~()]/g,"");
        var finalText = removePunc.replace(/\s{2,}/g, " ");
        var arrWords = finalText.split(" ");

        for (var i=0; i<arrWords.length-1; i++) {
            var word = arrWords[i];
            var next_word = arrWords[i+1];
            var bigram = word.concat(next_word);

            if (bigram) {
                emit(bigram, 1);
            }
        }
    }
};