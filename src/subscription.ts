import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return
    const ops = await getOpsByType(evt)

    // This logs the text of every post off the firehose.
    // Just for fun :)
    // Delete before actually using
    for (const post of ops.posts.creates) {
      console.log(post.record.text)
    }
    const aiKeywords = [
  'AI',
  'ML',
  'Artificial Intelligence',
  'Machine Learning',
  'Deep Learning',
  'Neural Networks',
  'Natural Language Processing',
  'Computer Vision',
  'Robotics',
  'Data Science',
  'Machine Learning',
  'Neural Networks',
  'Reinforcement Learning',
  'Supervised Learning',
  'Unsupervised Learning',
  'Generative Adversarial Networks',
  'Natural Language Generation',
  'Time Series Analysis',
  'Predictive Analytics',
  'Data Mining',
  'Data Visualization',
  'Recommender Systems',
  'Speech Recognition',
  'Image Recognition',
  ];
    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const postsToCreate = ops.posts.creates
      .filter((create) => {
        // only alf-related posts
    return aiKeywords.some((keyword) => {
    return create.record.text.toLowerCase().includes(keyword.toLowerCase());
    });
      .map((create) => {
        // map alf-related posts to a db row
        return {
          uri: create.uri,
          cid: create.cid,
          replyParent: create.record?.reply?.parent.uri ?? null,
          replyRoot: create.record?.reply?.root.uri ?? null,
          indexedAt: new Date().toISOString(),
        }
      })

    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }
    if (postsToCreate.length > 0) {
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
  }
}
