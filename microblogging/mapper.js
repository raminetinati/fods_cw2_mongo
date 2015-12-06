function mapper() {
    getUnigrams = function (text) {
        arrWords = text.toString().replace(/[.,]/g,"").split(" ");
        var arrNewWords = [];

        for (var i = 0; i < arrWords.length; i++) {
            var wordOccurence = arrWords[i];
            if(typeof arrNewWords[wordOccurence] === 'undefined') {
                var wordObj = {wordOccurence: 1};
                arrNewWords[arrWords[i]] = wordObj;
            } else {
                arrNewWords[arrWords[i]] = arrNewWords[wordOccurence]+1;
            }
        }
        return arrNewWords;
    };

    var unigrams = getUnigrams(this.text);

    emit(this.id, {unigrams: unigrams});
}