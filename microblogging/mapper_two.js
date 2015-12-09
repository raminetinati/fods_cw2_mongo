function mapper() {
    text = this.text
    if (text) {
        arrWords = text.toString().toLowerCase().replace(/[.,!?;:()%]/g, "").replace('\r', "").replace('\t').replace("[#](?![^#]*$)", "").split(" ");
        for (var i = arrWords.length - 1; i >= 0; i--) {
            word = arrWords[i]
            if (word) {
                emit(word, 1);
            }
        }
    }
};