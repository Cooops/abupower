/* append color-styling depending on the int being neutral, neg, or pos */
function colorizeDataAndInsert(colors, typeCheck) {
    // console.log(colors)
    // console.log(colors)
    if (colors == 0) {
        colors = `<span style='text-align: center !important; color: hsl(208, 9%, 38%); font-size: 1em;'>±${colors}%</span>`
    }
    else if (colors > 0) {
        colors = `<span style='text-align: center !important; color: rgb(143,198,132); font-size: 1em;'>+${colors}%</span>`
    }
    else {
        colors = `<span style='text-align: center !important; color: rgb(213,90,65); font-size: 1em;'>${colors}%</span>`
    }
    document.getElementById(typeCheck).innerHTML = colors;
};

function colorizeData(colors) {
    // console.log(colors)
    if (colors == 0) {
        colors = `<span style='text-align: center !important; color: hsl(208, 9%, 38%); font-size: 1em;'>±${colors}%</span>`
        return colors
    }
    else if (colors > 0) {
        colors = `<span style='text-align: center !important; color: rgb(143,198,132); font-size: 1em;'>+${colors}%</span>`
        return colors
    }
    else {
        colors = `<span style='text-align: center !important; color: rgb(213,90,65); font-size: 1em;'>${colors}%</span>`
        return colors
    }
};