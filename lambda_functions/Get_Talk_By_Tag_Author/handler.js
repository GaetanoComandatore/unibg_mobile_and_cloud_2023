const connect_to_db = require('./db');

// GET BY TALK HANDLER

const talk = require('./Talk');

module.exports.get_by_tag_author = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));
    let body = {};
    if (event.body) {
        body = JSON.parse(event.body);
    }
    // set default
    if(!body.tag && !body.main_author) {
        callback(null, {
                    statusCode: 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the talks. Tag is null.'
        });
    }
    
    if (!body.doc_per_page) {
        body.doc_per_page = 10;
    }
    if (!body.page) {
        body.page = 1;
    }
    
    connect_to_db().then(() => {
        console.log('=> get_all talks');
        if (!body.tag) {
            talk_list = talk.find({main_speaker: body.main_author})    
        } else if (!body.main_author) {
            talk_list= talk.find({tags: body.tag})
        } else {
            talk_list = talk.find({main_speaker: body.main_author, tags: body.tag})
        }
        
            talk_list.skip((body.doc_per_page * body.page) - body.doc_per_page)
            .limit(body.doc_per_page)
            .then(talks => {
                    callback(null, {
                        statusCode: 200,
                        body: JSON.stringify(talks)
                    });
                }
            )
            .catch(err =>
                callback(null, {
                    statusCode: err.statusCode || 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the talks.'
                })
            );
    });
};